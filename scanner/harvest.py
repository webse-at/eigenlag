"""Harvest-Schicht: GitHub-Code-Search nach Repo-Kandidaten fuer den Cross-Run-Scan.

Ein Treffer der Code-Search ist ein *Kandidat*, kein Signal. Die Volltext-Suche kennt
weder Kommentare noch `depends_on_past=False`. Ob wirklich eine Cross-Run-Kante vorliegt,
entscheidet erst die AST-Analyse in Session 002.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# GitHub-JSON ist Systemgrenze: das Schema gehoert nicht uns, deshalb Any statt TypedDict.
Json = dict[str, Any]

API = "https://api.github.com"
USER_AGENT = "eigenlag-scanner"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
HITS_PATH = DATA_DIR / "hits.jsonl"
CANDIDATES_PATH = DATA_DIR / "candidates.jsonl"
REJECTED_PATH = DATA_DIR / "rejected.jsonl"
ERRORS_PATH = DATA_DIR / "scan_errors.jsonl"
STATE_PATH = DATA_DIR / "harvest_state.json"

AIRFLOW_QUERIES = [
    "depends_on_past language:python",
    "wait_for_downstream language:python",
    "ExternalTaskSensor execution_delta language:python",
    "include_prior_dates language:python",
    "prev_start_date_success language:python",
]
DBT_QUERIES = ["is_incremental language:sql path:models"]
QUERIES = AIRFLOW_QUERIES + DBT_QUERIES

PER_PAGE = 100
MAX_PAGES = 10  # /search/code liefert hoechstens 1000 Ergebnisse pro Query.
MAX_SIZE_KB = 150 * 1024

BLOCKLIST = re.compile(
    r"awesome|tutorial|course|template|example|demo|playground|learning|training"
    r"|bootcamp|workshop|starter|boilerplate|cookiecutter|sandbox|test-repo",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Hit:
    """Ein Rohtreffer der Code-Search: Query, Repo, Datei."""

    query: str
    full_name: str
    path: str


@dataclass(frozen=True)
class Candidate:
    full_name: str
    html_url: str
    default_branch: str
    size_kb: int
    stars: int
    pushed_at: str
    matched_queries: list[str]
    matched_paths: list[str]

    @classmethod
    def build(cls, meta: Json, matched_queries: list[str], matched_paths: list[str]) -> Candidate:
        return cls(
            full_name=meta["full_name"],
            html_url=meta["html_url"],
            default_branch=meta["default_branch"],
            size_kb=meta["size"],
            stars=meta["stargazers_count"],
            pushed_at=meta["pushed_at"],
            matched_queries=matched_queries,
            matched_paths=matched_paths,
        )

    def as_dict(self) -> Json:
        return {
            "full_name": self.full_name,
            "html_url": self.html_url,
            "default_branch": self.default_branch,
            "size_kb": self.size_kb,
            "stars": self.stars,
            "pushed_at": self.pushed_at,
            "matched_queries": self.matched_queries,
            "matched_paths": self.matched_paths,
        }


def reject_reason(meta: Json) -> str | None:
    """Warum das Repo nicht in die Kandidatenliste gehoert, oder None."""
    if meta["fork"]:
        return "fork"
    if meta["archived"]:
        return "archived"
    if meta["size"] >= MAX_SIZE_KB:
        return "size"
    if BLOCKLIST.search(f"{meta['full_name']} {meta.get('description') or ''}"):
        return "blocklist"
    return None


def merge_hits(hits: Iterable[Hit]) -> dict[str, tuple[list[str], list[str]]]:
    """Rohtreffer zu einem Eintrag je Repo verdichten, Reihenfolge stabil."""
    merged: dict[str, tuple[list[str], list[str]]] = {}
    for hit in hits:
        queries, paths = merged.setdefault(hit.full_name, ([], []))
        if hit.query not in queries:
            queries.append(hit.query)
        if hit.path not in paths:
            paths.append(hit.path)
    return merged


def resolve_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    proc = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=False)
    token = proc.stdout.strip()
    if token:
        return token
    sys.exit(
        "Kein GitHub-Token gefunden. Entweder GITHUB_TOKEN setzen (export GITHUB_TOKEN=...) "
        "oder `gh auth login` ausfuehren. Ohne Token ist die Code-Search auf 10 statt 30 "
        "Requests pro Minute gedrosselt, der Lauf waere unbrauchbar langsam."
    )


def append_jsonl(path: Path, record: Json) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_jsonl(path: Path) -> Iterator[Json]:
    if not path.exists():
        return
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


class GitHub:
    """HTTP-Schicht mit proaktiver Drosselung. Fehler werden geloggt, nicht geworfen."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.limits: dict[str, tuple[int, int]] = {}  # Kontingent -> (remaining, reset_epoch)
        self.errors = 0

    def _wait_if_exhausted(self, resource: str) -> None:
        remaining, reset = self.limits.get(resource, (1, 0))
        if remaining > 0:
            return
        pause = max(0.0, reset - time.time()) + 1.0
        print(f"  Kontingent '{resource}' erschoepft, warte {pause:.0f}s", flush=True)
        time.sleep(pause)
        self.limits[resource] = (1, reset)

    def _remember_limits(self, headers: Any) -> None:  # http.client.HTTPMessage, kein stabiler Typ
        resource = headers.get("x-ratelimit-resource")
        remaining = headers.get("x-ratelimit-remaining")
        reset = headers.get("x-ratelimit-reset")
        if resource and remaining is not None and reset is not None:
            self.limits[resource] = (int(remaining), int(reset))

    def log_error(self, target: str, status: int | str, message: str) -> None:
        self.errors += 1
        append_jsonl(
            ERRORS_PATH,
            {
                "ts": datetime.now(UTC).isoformat(),
                "target": target,
                "status": status,
                "message": message[:500],
            },
        )

    def get(self, path: str, resource: str) -> Json | None:
        """GET auf die API. None heisst: Fehler, strukturiert geloggt, Lauf geht weiter."""
        url = f"{API}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": USER_AGENT,
            },
        )
        backoff = 2.0
        for attempt in range(5):
            self._wait_if_exhausted(resource)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    self._remember_limits(resp.headers)
                    body: Json = json.loads(resp.read())
                    return body
            except urllib.error.HTTPError as err:
                self._remember_limits(err.headers)
                text = err.read().decode("utf-8", "replace")
                if err.code in (403, 429) and err.headers.get("x-ratelimit-remaining") == "0":
                    reset = int(err.headers.get("x-ratelimit-reset", "0"))
                    pause = max(0.0, reset - time.time()) + 1.0
                    print(f"  {err.code} Rate-Limit, warte {pause:.0f}s", flush=True)
                    time.sleep(pause)
                    continue
                if err.code in (403, 429) and "secondary rate limit" in text.lower():
                    retry_after = err.headers.get("retry-after")
                    pause = float(retry_after) if retry_after else backoff
                    print(f"  Secondary Rate-Limit, Backoff {pause:.0f}s", flush=True)
                    time.sleep(pause)
                    backoff = min(backoff * 2, 60.0)
                    continue
                self.log_error(url, err.code, text)
                return None
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as err:
                if attempt == 4:
                    self.log_error(url, "network", repr(err))
                    return None
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
        self.log_error(url, "retries", "5 Versuche erschoepft")
        return None


def advance(entry: Json, n_items: int) -> None:
    """Seiten-Fortschaltung nach einer Antwort. Rein, damit der Deckel testbar ist.

    Eine Seite mit weniger als PER_PAGE Treffern ist die letzte der Query. Ohne diese
    Abbruchbedingung laeuft der Scanner in leere Seiten und verbrennt Requests gegen ein
    Kontingent von 30 pro Minute.
    """
    entry["next_page"] += 1
    if n_items < PER_PAGE or entry["next_page"] > MAX_PAGES:
        entry["done"] = True


def load_state() -> Json:
    if STATE_PATH.exists():
        state: Json = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return state
    return {"queries": {}}


def save_state(state: Json) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def search_all(gh: GitHub, state: Json) -> None:
    """Alle Queries paginiert abarbeiten: Rohtreffer nach hits.jsonl, Fortschritt nach state."""
    for query in QUERIES:
        entry = state["queries"].setdefault(query, {"next_page": 1, "done": False, "total": None})
        if entry["done"]:
            print(f"[fertig] {query} (aus vorigem Lauf)", flush=True)
            continue
        while not entry["done"]:
            page = entry["next_page"]
            qs = urllib.parse.urlencode({"q": query, "per_page": PER_PAGE, "page": page})
            body = gh.get(f"/search/code?{qs}", resource="search")
            if body is None:
                print(f"[fehler] {query} Seite {page} uebersprungen, siehe scan_errors.jsonl")
                break
            entry["total"] = body["total_count"]
            items = body["items"]
            for item in items:
                append_jsonl(
                    HITS_PATH,
                    {
                        "query": query,
                        "full_name": item["repository"]["full_name"],
                        "path": item["path"],
                    },
                )
            print(
                f"[suche] {query} | Seite {page}: {len(items)} Treffer "
                f"(total_count {entry['total']})",
                flush=True,
            )
            advance(entry, len(items))
            save_state(state)


def fetch_metadata(gh: GitHub) -> None:
    """Fuer jedes noch unbewertete Repo Metadaten holen, filtern, Ergebnis schreiben."""
    merged = merge_hits(Hit(**h) for h in read_jsonl(HITS_PATH))
    seen = {rec["full_name"] for rec in read_jsonl(CANDIDATES_PATH)}
    seen |= {rec["full_name"] for rec in read_jsonl(REJECTED_PATH)}
    todo = [name for name in merged if name not in seen]
    print(f"\n[meta] {len(merged)} Repos in hits.jsonl, {len(todo)} noch zu bewerten", flush=True)

    for i, full_name in enumerate(todo, 1):
        meta = gh.get(f"/repos/{full_name}", resource="core")
        if meta is None:
            continue  # geloggt, der naechste Lauf versucht es erneut
        queries, paths = merged[full_name]
        reason = reject_reason(meta)
        if reason:
            append_jsonl(
                REJECTED_PATH,
                {
                    "full_name": full_name,
                    "reason": reason,
                    "size_kb": meta["size"],
                    "fork": meta["fork"],
                    "archived": meta["archived"],
                    "description": meta.get("description"),
                    "matched_queries": queries,
                },
            )
        else:
            append_jsonl(CANDIDATES_PATH, Candidate.build(meta, queries, paths).as_dict())
        if i % 50 == 0:
            print(f"  {i}/{len(todo)} bewertet", flush=True)


def summarize() -> None:
    candidates = list(read_jsonl(CANDIDATES_PATH))
    rejected = list(read_jsonl(REJECTED_PATH))
    airflow = [c for c in candidates if set(c["matched_queries"]) & set(AIRFLOW_QUERIES)]
    dbt = [c for c in candidates if set(c["matched_queries"]) & set(DBT_QUERIES)]

    reasons: dict[str, int] = {}
    for rec in rejected:
        reasons[rec["reason"]] = reasons.get(rec["reason"], 0) + 1
    total = len(candidates) + len(rejected)

    print("\n=== Harvest-Ergebnis ===")
    print(f"Repos bewertet:   {total}")
    print(f"Kandidaten:       {len(candidates)}")
    print(f"  davon Airflow:  {len(airflow)}")
    print(f"  davon dbt:      {len(dbt)}")
    print(f"Verworfen:        {len(rejected)}")
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        share = count / total * 100 if total else 0.0
        print(f"  {reason:<10} {count:>5}  ({share:.1f} % der bewerteten Repos)")
    print(f"\nKandidaten: {CANDIDATES_PATH}")
    print(f"Verworfen:  {REJECTED_PATH}")
    print(f"Fehler:     {ERRORS_PATH}")


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    gh = GitHub(resolve_token())
    state = load_state()
    search_all(gh, state)
    fetch_metadata(gh)
    summarize()
    if gh.errors:
        print(f"\n{gh.errors} Fehler in diesem Lauf, siehe {ERRORS_PATH}")


if __name__ == "__main__":
    main()
