"""Duenne Kompositionsfunktion (Spec 008): parsen, Dauern heiraten, kondensieren,
Howard — das erste Lambda in Sekunden. CLI und Report sind Session 009.

Die Pflicht-Warnung fuer Sensoren auf dem kritischen Kreis lebt hier, nicht in der
Dauern-Schicht: erst der Kreis entscheidet, ob die Sensor-Dauer Lambda traegt. Die
gemessene Dauer eines Sensors ist Wartezeit auf die Wanduhr (wiki/math.md, Abschnitt 9);
sie laesst sich aus der Metadaten-DB nicht von Arbeitszeit trennen, also wird nicht so
getan — markieren statt herausrechnen.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from eigenlag.durations import DurationWarning, Statistic, TaskStats, resolve
from eigenlag.maxplus import CondensedEdge, condense, critical_path, howard
from eigenlag.model import Pipeline
from eigenlag.parse_airflow import ParseResult, Warning_, node_name, parse_path, to_pipeline

SENSOR_CYCLE_TEXT = (
    "Kreis enthaelt Wartezeit auf externe Ereignisse;"
    " Lambda kann ueberschaetzt sein und ist keine harte Untergrenze mehr"
)


@dataclass(frozen=True)
class Analysis:
    pipeline: Pipeline
    lam: float | None  # None: kein Kreis ueber die Zeitachse, nicht anwendbar (ADR-007)
    cycle: tuple[CondensedEdge, ...] | None
    cycle_tasks: tuple[str, ...]  # aufgeloester Task-Pfad des Kreises (ADR-002)
    critical_path_s: float
    critical_path_tasks: list[str]
    parse_warnings: tuple[Warning_, ...]
    warnings: tuple[DurationWarning, ...]


def analyze(
    path: Path,
    stats: Mapping[str, TaskStats],
    statistic: Statistic = "mean",
    fallback: TaskStats | None = None,
) -> Analysis:
    return analyze_result(parse_path(path), stats, statistic, fallback)


def analyze_result(
    result: ParseResult,
    stats: Mapping[str, TaskStats],
    statistic: Statistic = "mean",
    fallback: TaskStats | None = None,
) -> Analysis:
    """Wie analyze(), aber auf einem bereits geparsten Satz: die CLI parst zuerst
    (DAG-Filter, dag_ids fuer die Metadaten-Query) und analysiert danach."""
    parse_warnings = result.warnings + tuple(w for dag in result.dags for w in dag.warnings)

    nodes = [node_name(dag, task) for dag in result.dags for task in dag.tasks]
    durations, duration_warnings = resolve(nodes, stats, statistic, fallback)
    pipeline = to_pipeline(result.dags, durations)

    graph, paths = condense(pipeline)
    cp_length, cp_tasks = critical_path(pipeline)

    outcome = howard(graph)
    if outcome is None:
        return Analysis(
            pipeline=pipeline,
            lam=None,
            cycle=None,
            cycle_tasks=(),
            critical_path_s=cp_length,
            critical_path_tasks=cp_tasks,
            parse_warnings=parse_warnings,
            warnings=duration_warnings,
        )

    lam, cycle = outcome
    cycle_tasks: list[str] = []
    for edge in cycle:
        for task in paths[(edge.src, edge.dst, edge.periods)]:
            if task not in cycle_tasks:
                cycle_tasks.append(task)

    sensor_warnings = tuple(
        DurationWarning(kind="sensor_im_kritischen_kreis", task=task, detail=SENSOR_CYCLE_TEXT)
        for task in cycle_tasks
        if (found := stats.get(task)) is not None and found.is_sensor
    )
    return Analysis(
        pipeline=pipeline,
        lam=lam,
        cycle=tuple(cycle),
        cycle_tasks=tuple(cycle_tasks),
        critical_path_s=cp_length,
        critical_path_tasks=cp_tasks,
        parse_warnings=parse_warnings,
        warnings=duration_warnings + sensor_warnings,
    )
