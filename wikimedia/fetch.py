"""Prometheus-Abfragen gegen Wikimedias Grafana-Proxy, mit Cache auf Disk.

Fremde Infrastruktur: read-only, jede Abfrage genau einmal, danach aus dem Cache. Der Cache
liegt unter `data/wikimedia/cache/` und ist der Beleg fuer jede Zahl im Report. Fehler (HTTP
!= 200, leere Serie, Prometheus-Fehlerstatus) landen in `data/wikimedia/fetch_errors.jsonl`,
sie werden nicht geraten.

Das ist Systemgrenze, hier gehoert Validierung hin (CLAUDE.md, Code-Konventionen).
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

Json = dict[str, Any]  # Prometheus-Antworten gehen roh in den Cache, kein festes Schema.

PROXY = "https://grafana.wikimedia.org/api/datasources/proxy/uid/000000026/api/v1"
USER_AGENT = "eigenlag-research/0.1 (read-only; https://github.com/webse-at)"

CACHE_DIR = Path("data/wikimedia/cache")
ERROR_LOG = Path("data/wikimedia/fetch_errors.jsonl")

MIN_INTERVAL = 1.0  # Sekunden zwischen zwei echten Requests. Wir sind Gast.
MAX_POINTS = 10_000  # Prometheus lehnt query_range ueber 11.000 Punkten ab.
TIMEOUT = 120.0


class FetchError(RuntimeError):
    """Die Abfrage ist fehlgeschlagen und wurde protokolliert."""


@dataclass
class Fetcher:
    """Kapselt Cache, Rate-Limit und Fehlerprotokoll. `offline` verbietet echte Requests."""

    cache_dir: Path = CACHE_DIR
    error_log: Path = ERROR_LOG
    offline: bool = False
    requests_made: int = 0
    cache_hits: int = 0
    _last_request: float = 0.0

    def _cache_path(self, path: str, params: dict[str, str]) -> Path:
        key = json.dumps([path, sorted(params.items())], sort_keys=True)
        digest = hashlib.sha256(key.encode()).hexdigest()[:16]
        return self.cache_dir / f"{path.strip('/').replace('/', '_')}_{digest}.json"

    def _log_error(self, record: Json) -> None:
        self.error_log.parent.mkdir(parents=True, exist_ok=True)
        with self.error_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def _get(self, path: str, params: dict[str, str]) -> Json:
        url = f"{PROXY}/{path}?{urllib.parse.urlencode(params)}"
        wait = MIN_INTERVAL - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                status = response.status
                body = response.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError) as err:
            self._log_error({"path": path, "params": params, "error": str(err)[:300]})
            raise FetchError(f"{path}: {err}") from err
        finally:
            self._last_request = time.monotonic()
            self.requests_made += 1

        if status != 200:
            self._log_error({"path": path, "params": params, "status": status})
            raise FetchError(f"{path}: HTTP {status}")
        payload: Json = json.loads(body)
        if payload.get("status") != "success":
            self._log_error({"path": path, "params": params, "payload": payload})
            raise FetchError(f"{path}: Prometheus-Status {payload.get('status')!r}")
        return payload

    def get(self, path: str, params: dict[str, str]) -> Json:
        """Cache-first. Ein Treffer im Cache macht keinen Request."""
        cached = self._cache_path(path, params)
        if cached.exists():
            self.cache_hits += 1
            record: Json = json.loads(cached.read_text(encoding="utf-8"))
            payload: Json = record["payload"]
            return payload
        if self.offline:
            raise FetchError(f"offline und nicht im Cache: {path} {params}")

        payload = self._get(path, params)
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_text(
            json.dumps(
                {
                    "path": path,
                    "params": params,
                    "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "payload": payload,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return payload

    def label_values(self, label: str) -> list[str]:
        payload = self.get(f"label/{label}/values", {})
        values: list[str] = payload["data"]
        return values

    def series_labels(self, selector: str) -> list[Json]:
        """Welche Serien gibt es zu diesem Selektor, und welche Labels tragen sie?"""
        payload = self.get("series", {"match[]": selector})
        series: list[Json] = payload["data"]
        return series

    def query(self, promql: str, at: int | None = None) -> list[Json]:
        params = {"query": promql}
        if at is not None:
            params["time"] = str(at)
        payload = self.get("query", params)
        result: list[Json] = payload["data"]["result"]
        if not result:
            self._log_error({"path": "query", "params": params, "error": "leere Serie"})
        return result

    def query_range(self, promql: str, start: int, end: int, step: int) -> list[Json]:
        """Zeitreihe ueber ein Fenster. Wird in Bloecke zerlegt, wenn Prometheus sonst abweist.

        Die Bloecke werden einzeln gecacht und hier wieder zusammengesetzt: ein Abbruch
        mittendrin kostet nur den fehlenden Block.
        """
        span = MAX_POINTS * step
        merged: dict[str, Json] = {}
        for block_start in range(start, end, span):
            block_end = min(block_start + span, end)
            params = {
                "query": promql,
                "start": str(block_start),
                "end": str(block_end),
                "step": str(step),
            }
            payload = self.get("query_range", params)
            for series in payload["data"]["result"]:
                key = json.dumps(series["metric"], sort_keys=True)
                if key not in merged:
                    merged[key] = {"metric": series["metric"], "values": []}
                merged[key]["values"].extend(series["values"])

        if not merged:
            self._log_error(
                {"path": "query_range", "query": promql, "error": "leere Serie", "start": start}
            )
        for series in merged.values():
            deduped: dict[float, str] = {}
            for timestamp, value in series["values"]:
                deduped[timestamp] = value  # Blockgrenzen liefern denselben Punkt zweimal.
            series["values"] = [[t, deduped[t]] for t in sorted(deduped)]
        return list(merged.values())
