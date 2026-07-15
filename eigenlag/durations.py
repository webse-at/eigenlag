"""Dauern-Schicht (Spec 008): beschafft echte Task-Dauern, macht aus Struktur-Aussagen
Zeit-Aussagen.

Drei Quellen, eine Ausgabeform: `from_metadata_db` (Airflow-Metadaten-DB via sqlalchemy,
optionales Extra `eigenlag[db]`), `from_rest` (Airflow-Stable-API via urllib) und
`assume` (fester Wert als Fallback). Je Task p50/p95/mean/n/operator/is_sensor; welche
Statistik in Lambda eingeht, entscheidet der Aufrufer (`pick`/`resolve`), Default `mean`:
fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige Groesse
(wiki/math.md), aber er ist ausreisser-empfindlich (Abnahme 005a: ein 4,6-Tage-Haenger
verschiebt ihn um ~560 s) — deshalb liefert die Schicht immer alle drei.

Sensor-Dauern werden markiert, nicht herausgerechnet: gemessene Sensor-Dauer ist
Wartezeit auf die Wanduhr (wiki/math.md, Abschnitt 9), und aus der Metadaten-DB laesst
sich Warten nicht von Arbeiten trennen. Die Pflicht-Warnung haengt analyze() an, sobald
ein Sensor auf dem kritischen Kreis liegt.

Schema-Annahmen (task_instance: dag_id, task_id, state, duration in Sekunden, operator,
start_date; task_id traegt TaskGroup-Prefixe) sind gegen Airflow standalone verifiziert,
Beleg in wiki/log.md, Session 008.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

Statistic = Literal["mean", "p50", "p95"]

MIN_SAMPLE = 5


@dataclass(frozen=True)
class TaskStats:
    p50: float
    p95: float
    mean: float
    n: int
    operator: str | None

    @property
    def is_sensor(self) -> bool:
        # "Sensor" im Klassennamen statt endswith: DateTimeSensorAsync u. ae.
        return self.operator is not None and "Sensor" in self.operator


@dataclass(frozen=True)
class DurationWarning:
    kind: str
    task: str
    detail: str = ""


Stats = dict[str, TaskStats]


def _percentile(values: Sequence[float], q: float) -> float:
    """Lineare Interpolation auf sortierten Werten, identisch mit percentile_cont."""
    ordered = sorted(values)
    h = q * (len(ordered) - 1)
    lo = int(h)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (h - lo) * (ordered[hi] - ordered[lo])


def _stats_of(durations: Sequence[float], operator: str | None) -> TaskStats:
    return TaskStats(
        p50=_percentile(durations, 0.5),
        p95=_percentile(durations, 0.95),
        mean=sum(durations) / len(durations),
        n=len(durations),
        operator=operator,
    )


def assume(seconds: float) -> TaskStats:
    """Fester Wert fuer jede Task ohne Messung (--assume-duration). n=0 sagt ehrlich:
    das ist eine Annahme, keine Statistik."""
    return TaskStats(p50=seconds, p95=seconds, mean=seconds, n=0, operator=None)


def pick(stats: Mapping[str, TaskStats], statistic: Statistic) -> dict[str, float]:
    return {task: getattr(s, statistic) for task, s in stats.items()}


def resolve(
    tasks: Iterable[str],
    stats: Mapping[str, TaskStats],
    statistic: Statistic = "mean",
    fallback: TaskStats | None = None,
    min_n: int = MIN_SAMPLE,
) -> tuple[dict[str, float], tuple[DurationWarning, ...]]:
    """Baut das durations-Mapping fuer to_pipeline. Mischbetrieb ist der Normalfall:
    fehlende Tasks und Tasks unter der Mindest-Stichprobe bekommen den Fallback,
    je Task mit Warnung — stillschweigend 0 waere eine Luege Richtung "kein Problem"."""
    durations: dict[str, float] = {}
    warnings: list[DurationWarning] = []
    for task in tasks:
        found = stats.get(task)
        if found is not None and found.n >= min_n:
            durations[task] = getattr(found, statistic)
            continue
        if fallback is None:
            reason = "no measurement" if found is None else f"only {found.n} runs"
            raise ValueError(
                f"task {task!r}: {reason} and no assume fallback (--assume-duration) set"
            )
        durations[task] = getattr(fallback, statistic)
        if found is None:
            warnings.append(
                DurationWarning(
                    kind="dauer_angenommen",
                    task=task,
                    detail=f"no measurement, {getattr(fallback, statistic)} s",
                )
            )
        else:
            warnings.append(
                DurationWarning(
                    kind="stichprobe_zu_klein",
                    task=task,
                    detail=f"n={found.n} < {min_n}, assumed value {getattr(fallback, statistic)} s",
                )
            )
    return durations, tuple(warnings)


# --- Quelle 1: Airflow-Metadaten-DB ---------------------------------------------------


def from_metadata_db(
    url: str, dag_ids: Sequence[str], since_days: int = 90
) -> tuple[Stats, tuple[DurationWarning, ...]]:
    """Aggregiert task_instance je (dag_id, task_id) ueber erfolgreiche Laeufe.

    Auf PostgreSQL rechnet die DB die Perzentile selbst (percentile_cont — bei
    Millionen Zeilen gehoert die Aggregation in die DB), sonst holt die Query die
    Dauern und Python aggregiert (SQLite kennt kein percentile_cont).
    """
    try:
        import sqlalchemy as sa
    except ImportError as exc:  # Systemgrenze: optionale Dependency
        raise ImportError(
            "from_metadata_db braucht sqlalchemy: pip install 'eigenlag[db]'"
        ) from exc

    engine = sa.create_engine(url)
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=since_days)
    stats: Stats = {}
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            rows = conn.execute(
                sa.text(
                    "SELECT dag_id, task_id, count(*) AS n, avg(duration) AS mean,"
                    " percentile_cont(0.5) WITHIN GROUP (ORDER BY duration) AS p50,"
                    " percentile_cont(0.95) WITHIN GROUP (ORDER BY duration) AS p95,"
                    " min(operator) AS operator"
                    " FROM task_instance"
                    " WHERE state = 'success' AND duration IS NOT NULL"
                    " AND dag_id = ANY(:dag_ids) AND start_date >= :cutoff"
                    " GROUP BY dag_id, task_id"
                ),
                {"dag_ids": list(dag_ids), "cutoff": cutoff},
            )
            for row in rows:
                stats[f"{row.dag_id}.{row.task_id}"] = TaskStats(
                    p50=float(row.p50),
                    p95=float(row.p95),
                    mean=float(row.mean),
                    n=int(row.n),
                    operator=row.operator,
                )
        else:
            placeholders = ", ".join(f":d{i}" for i in range(len(dag_ids)))
            # Airflow legt start_date in SQLite als naiven UTC-String ab
            # ("2026-07-15 06:45:46.635753", gegen Airflow 3.3.0 verifiziert) —
            # der Vergleich laeuft lexikografisch, also dasselbe Format binden.
            rows = conn.execute(
                sa.text(
                    "SELECT dag_id, task_id, duration, operator FROM task_instance"
                    " WHERE state = 'success' AND duration IS NOT NULL"
                    f" AND dag_id IN ({placeholders}) AND start_date >= :cutoff"
                ),
                {
                    "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    **{f"d{i}": d for i, d in enumerate(dag_ids)},
                },
            )
            per_task: dict[str, tuple[list[float], str | None]] = {}
            for row in rows:
                durations, operator = per_task.setdefault(
                    f"{row.dag_id}.{row.task_id}", ([], row.operator)
                )
                durations.append(float(row.duration))
            for task, (durations, operator) in per_task.items():
                stats[task] = _stats_of(durations, operator)
    return stats, ()


# --- Quelle 2: Airflow-Stable-API (REST) ----------------------------------------------


def _auth_header(auth: tuple[str, str] | str) -> str:
    if isinstance(auth, tuple):
        user, password = auth
        token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
        return f"Basic {token}"
    return f"Bearer {auth}"


def _get_json(url: str, headers: dict[str, str]) -> Any:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def from_rest(
    base_url: str,
    auth: tuple[str, str] | str,
    dag_ids: Sequence[str],
    since_days: int = 90,
    page_size: int = 100,
    max_pages: int = 50,
    min_interval_s: float = 0.5,
    api_version: str = "v2",
) -> tuple[Stats, tuple[DurationWarning, ...]]:
    """Holt Task-Instanzen ueber GET /api/{version}/dags/{dag_id}/dagRuns/~/taskInstances.

    Gegen Airflow 3.3.0 verifiziert (wiki/log.md, Session 008): Airflow 3 hat /api/v1
    entfernt und Basic Auth abgeschafft — es gilt /api/v2 mit JWT-Bearer-Token
    (POST /auth/token liefert ihn). Fuer Airflow 2 gilt api_version="v1", dort geht
    auch Basic Auth (User/Passwort-Tupel). Die Antwort-Felder sind in beiden Versionen
    strukturgleich (task_id, state, duration, operator).

    Die API ist der Live-Scheduler, kein Data Warehouse: maximal 2 Requests/s
    (min_interval_s), Abbruch nach max_pages je DAG mit Warnung statt endlosem Crawl.
    """
    headers = {"Authorization": _auth_header(auth), "Accept": "application/json"}
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=since_days)
    warnings: list[DurationWarning] = []
    per_task: dict[str, tuple[list[float], str | None]] = {}
    last_request = 0.0

    for dag_id in dag_ids:
        total: int | None = None
        for page in range(max_pages):
            wait = min_interval_s - (time.monotonic() - last_request)
            if wait > 0:
                time.sleep(wait)
            query = urllib.parse.urlencode(
                {
                    "limit": page_size,
                    "offset": page * page_size,
                    "start_date_gte": cutoff.isoformat(),
                }
            )
            url = f"{base_url}/api/{api_version}/dags/{dag_id}/dagRuns/~/taskInstances?{query}"
            payload = _get_json(url, headers)
            last_request = time.monotonic()
            total = int(payload["total_entries"])
            for ti in payload["task_instances"]:
                if ti.get("state") != "success" or ti.get("duration") is None:
                    continue
                durations, _ = per_task.setdefault(
                    f"{dag_id}.{ti['task_id']}", ([], ti.get("operator"))
                )
                durations.append(float(ti["duration"]))
            if (page + 1) * page_size >= total:
                break
        else:
            warnings.append(
                DurationWarning(
                    kind="rest_seiten_deckel",
                    task="",
                    detail=(
                        f"DAG {dag_id!r}: stopped after {max_pages} pages, {total} entries total"
                    ),
                )
            )

    stats = {task: _stats_of(durations, op) for task, (durations, op) in per_task.items()}
    return stats, tuple(warnings)
