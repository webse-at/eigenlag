"""Aus Prometheus-Gauges einzelne Laeufe rekonstruieren.

`airflow_dagrun_duration` ist eine Gauge, keine Verteilung: Airflow meldet am Ende eines
Laufs dessen Dauer an StatsD, der Exporter haelt den Wert und Prometheus scrapt ihn. Ein
`avg_over_time` darueber mittelt also Scrapes, nicht Laeufe, und ist nur dann die mittlere
Laufzeit, wenn jeder Lauf gleich viele Scrapes beitraegt. Das ist eine Annahme, keine
Zusicherung, und sie wird hier nicht gemacht.

Stattdessen wird der Lauf selbst rekonstruiert: aufeinanderfolgende Rohsamples mit
identischem Wert sind derselbe Lauf, mehrfach gescrapt. Ein Wechsel des Werts ist ein neuer
Lauf. Die Dauern sind Fliesskomma-Millisekunden mit Sub-Millisekunden-Stellen, zwei Laeufe
mit exakt gleichem Wert sind darum praktisch ausgeschlossen. Wo es sie doch gibt, zaehlen wir
einen Lauf zu wenig, und das ist die Richtung, in der ein Fehler uns nicht nuetzt.

Belegt wird diese Lesart durch eine unabhaengige Groesse: der mediane Abstand zwischen zwei
rekonstruierten Laeufen muss zur medianen Laufdauer passen, wenn die Laeufe seriell liegen
(`max_active_runs=1`). Siehe `wikimedia/case.md`.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any

Json = dict[str, Any]

MS = 1000.0  # Airflow meldet StatsD-Timings in Millisekunden.
OUTAGE_GAP = 4 * 3600.0  # Laengere Luecke: die Metrik fehlt, das ist kein langer Lauf.
SAME_RUN = 300.0  # Zwei Pods melden denselben Lauf: gleiche Dauer, wenige Scrapes auseinander.


@dataclass(frozen=True)
class Run:
    """Ein Lauf, erkannt am Wechsel des Gauge-Werts. `at` ist der erste Scrape des Werts."""

    at: float
    duration: float  # Sekunden


@dataclass(frozen=True)
class Window:
    """Ein Stueck Zeitreihe ohne Metrik-Luecke."""

    start: float
    end: float
    runs: tuple[Run, ...]

    @property
    def span(self) -> float:
        return self.end - self.start

    @property
    def cadence(self) -> float | None:
        """Beobachteter Takt: mittlerer Abstand zwischen zwei Laufenden."""
        if len(self.runs) < 2:
            return None
        return self.span / (len(self.runs) - 1)


def samples(series: list[Json]) -> list[tuple[float, float]]:
    """Alle Rohsamples aller Serien, nach Zeit sortiert. Nur fuer Groessen ohne Lauf-Logik."""
    points: dict[float, float] = {}
    for entry in series:
        for timestamp, value in entry["values"]:
            points[float(timestamp)] = float(value)
    return [(t, points[t]) for t in sorted(points)]


def _runs_of_one(entry: Json) -> list[Run]:
    runs: list[Run] = []
    previous: float | None = None
    for timestamp, raw in sorted(entry["values"], key=lambda point: float(point[0])):
        value = float(raw)
        if previous is None or value != previous:
            runs.append(Run(at=float(timestamp), duration=value / MS))
        previous = value
    return runs


def extract_runs(series: list[Json]) -> list[Run]:
    """Wertwechsel der Gauge sind Laeufe. Gleicher Wert hintereinander ist derselbe Lauf.

    Der Wertwechsel wird **je Serie** gesucht, nicht auf der zusammengefuehrten Zeitachse.
    Wikimedia betreibt mehrere StatsD-Pods; jeder haelt seinen eigenen letzten Wert. Legt man
    ihre Samples uebereinander, springt die Reihe zwischen den Pods hin und her, und jeder
    Sprung saehe aus wie ein Lauf. Bei `refine_api_requests_hourly` ergab das ueber 30 Tage
    3362 statt 700 Laeufe: ein stuendlicher DAG mit angeblich fuenf Laeufen je Stunde.

    Zwei Pods, die denselben Lauf melden, ergeben zwei gleiche Runs kurz hintereinander. Die
    werden zusammengefasst: gleiche Dauer innerhalb von `SAME_RUN` ist derselbe Lauf.
    """
    found = sorted(
        ((run, index) for index, entry in enumerate(series) for run in _runs_of_one(entry)),
        key=lambda pair: pair[0].at,
    )
    merged: list[tuple[Run, int]] = []
    for run, index in found:
        twin = any(
            other != index and run.duration == seen.duration and run.at - seen.at <= SAME_RUN
            for seen, other in merged[-8:]
        )
        if not twin:
            merged.append((run, index))
    return [run for run, _ in merged]


def windows(runs: list[Run], outage_gap: float = OUTAGE_GAP) -> list[Window]:
    """Die Laeufe in Fenster ohne Metrik-Luecke zerlegen.

    Eine Luecke von Stunden heisst, dass die Metrik fehlte, nicht dass kein Lauf lief. Ueber
    eine solche Luecke hinweg einen Takt zu mitteln, waere eine erfundene Zahl.
    """
    if not runs:
        return []
    blocks: list[list[Run]] = [[runs[0]]]
    for previous, current in zip(runs, runs[1:], strict=False):
        if current.at - previous.at > outage_gap:
            blocks.append([current])
        else:
            blocks[-1].append(current)
    return [Window(start=b[0].at, end=b[-1].at, runs=tuple(b)) for b in blocks]


@dataclass(frozen=True)
class Stats:
    n: int
    median: float
    mean: float
    p95: float
    minimum: float
    maximum: float


def stats(values: list[float]) -> Stats:
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, round(0.95 * len(ordered)) - 1))
    return Stats(
        n=len(ordered),
        median=statistics.median(ordered),
        mean=statistics.fmean(ordered),
        p95=ordered[rank],
        minimum=ordered[0],
        maximum=ordered[-1],
    )
