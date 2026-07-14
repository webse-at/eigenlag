"""Voller Scan-Lauf ueber die Kandidatenliste: klonen, analysieren, Zwischenstand auf Disk.

Resume-faehig (CLAUDE.md, Regel 8): jedes Repo bekommt ein eigenes State-File. Ein zweiter
Lauf ueberspringt, was schon liegt. Fehler kippen den Lauf nicht, sie gehen strukturiert nach
`scan_errors.jsonl` (Regel 7).

Der Commit-SHA des Clones wird mitgeschrieben, weil der Permalink im Report sonst mit dem
Branch mitwandert und der Beleg verrottet (Regel 6).
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scanner.analyze import STRONG_KINDS, DagFinding, Signal, analyze_repo
from scanner.analyze_dbt import analyze_dbt_repo
from scanner.clone import ensure_clone

Json = dict[str, Any]  # Kandidaten-JSONL und State-Records sind Ein-/Ausgabe-Schema, kein Modell.

DBT_QUERY_MARKER = "is_incremental"
SKIP_DIRS = {".git", "node_modules", "venv", ".venv", "target", "dbt_packages"}


def read_candidates(path: Path) -> list[Json]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def state_path(state_dir: Path, full_name: str) -> Path:
    return state_dir / f"{full_name.replace('/', '__')}.json"


def head_sha(repo_dir: Path) -> str | None:
    proc = subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout.strip() if proc.returncode == 0 else None


def dbt_projects(root: Path) -> list[Path]:
    """Jedes Verzeichnis mit `dbt_project.yml` ist ein dbt-Projekt, auch im Unterordner.

    Ohne diese Datei ist ein Ordner `models/` mit SQL darin kein dbt-Projekt, und seine
    Dateien duerfen nicht als Models gezaehlt werden.
    """
    found = []
    for path in sorted(root.rglob("dbt_project.yml")):
        if SKIP_DIRS & set(path.relative_to(root).parts) or not path.is_file():
            continue
        found.append(path.parent)
    return found


class ErrorLog:
    """Append-only JSONL, aus mehreren Threads beschrieben."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, record: Json) -> None:
        line = json.dumps(record, ensure_ascii=False)
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def _signal_json(signal: Signal, prefix: str = "") -> Json:
    record = asdict(signal)
    record["file"] = f"{prefix}{signal.file}"
    return record


def _dag_json(finding: DagFinding) -> Json:
    return {
        "dag_id": finding.dag_id,
        "file": finding.file,
        "lineno": finding.lineno,
        "schedule_class": finding.schedule,
        "schedule_raw": finding.schedule_expr,
        "task_count": finding.task_count,
        "signals": [_signal_json(s) for s in finding.signals],
    }


def scan_repo(candidate: Json, repos_dir: Path, errors: ErrorLog) -> Json:
    full_name = str(candidate["full_name"])
    record: Json = {
        "repo": full_name,
        "queries": candidate.get("matched_queries", []),
        "stars": candidate.get("stars"),
        "clone_ok": False,
        "sha": None,
        "dags": [],
        "factories": [],
        "dbt": {"projects": 0, "models": 0, "signals": []},
        "files_parsed": 0,
        "syntax_errors": 0,
        "errors": [],
    }

    root = ensure_clone(
        full_name,
        str(candidate["html_url"]),
        str(candidate["default_branch"]),
        repos_dir,
        on_error=errors.write,
    )
    if root is None:
        return record

    record["clone_ok"] = True
    record["sha"] = head_sha(root)

    analysis = analyze_repo(root, full_name)
    record["files_parsed"] = analysis.files_parsed
    record["syntax_errors"] = analysis.syntax_errors
    record["dags"] = [_dag_json(d) for d in analysis.dags]
    record["factories"] = [_signal_json(s) for s in analysis.factories]

    for project in dbt_projects(root):
        prefix = project.relative_to(root).as_posix()
        prefix = f"{prefix}/" if prefix else ""
        dbt = analyze_dbt_repo(project, full_name)
        record["dbt"]["projects"] += 1
        record["dbt"]["models"] += dbt.models
        record["dbt"]["signals"].extend(
            {
                "path": f"{prefix}{s.path}",
                "lineno": s.lineno,
                "materialized_from": s.materialized_from,
            }
            for s in dbt.signals
        )
        for err in dbt.errors:
            analysis.errors.append({"repo": full_name, **err, "file": f"{prefix}{err['file']}"})

    record["errors"] = analysis.errors
    for err in analysis.errors:
        errors.write(err)
    return record


def pending(candidates: list[Json], state_dir: Path) -> Iterator[Json]:
    for candidate in candidates:
        if not state_path(state_dir, str(candidate["full_name"])).exists():
            yield candidate


def run(
    candidates: list[Json],
    repos_dir: Path,
    state_dir: Path,
    errors_path: Path,
    workers: int,
) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    errors = ErrorLog(errors_path)
    todo = list(pending(candidates, state_dir))
    done = len(candidates) - len(todo)
    print(
        f"{len(candidates)} Kandidaten, {done} bereits analysiert, {len(todo)} offen, "
        f"{workers} Worker",
        flush=True,
    )

    started = time.monotonic()
    counter = 0
    lock = threading.Lock()

    def work(candidate: Json) -> None:
        nonlocal counter
        record = scan_repo(candidate, repos_dir, errors)
        state_path(state_dir, str(candidate["full_name"])).write_text(
            json.dumps(record, ensure_ascii=False), encoding="utf-8"
        )
        risk = sum(
            1
            for d in record["dags"]
            if d["schedule_class"] == "subdaily"
            and any(s["kind"] in STRONG_KINDS for s in d["signals"])
        )
        with lock:
            counter += 1
            elapsed = time.monotonic() - started
            print(
                f"[{counter}/{len(todo)}] {record['repo']}: "
                f"{len(record['dags'])} DAGs, {risk} Risiko, "
                f"{len(record['dbt']['signals'])} dbt-Signale, "
                f"{len(record['factories'])} Factory-Signale"
                f"{'' if record['clone_ok'] else ', CLONE FEHLGESCHLAGEN'} "
                f"({elapsed:.0f}s)",
                flush=True,
            )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(work, todo))

    print(f"Fertig: {counter} Repos in {time.monotonic() - started:.0f}s", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scan-Lauf ueber die Kandidatenliste")
    parser.add_argument("--candidates", type=Path, default=Path("data/candidates.jsonl"))
    parser.add_argument("--repos", type=Path, default=Path("data/repos"))
    parser.add_argument("--state", type=Path, default=Path("data/scan_state"))
    parser.add_argument("--errors", type=Path, default=Path("data/scan_errors.jsonl"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--sample", type=int, default=0, help="zufaellige Teilmenge, nicht die ersten n"
    )
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)

    candidates = read_candidates(args.candidates)
    if args.sample:
        candidates = random.Random(args.seed).sample(candidates, min(args.sample, len(candidates)))
    run(candidates, args.repos, args.state, args.errors, args.workers)
    return 0


if __name__ == "__main__":
    sys.exit(main())
