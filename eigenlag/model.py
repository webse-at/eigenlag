"""Datentypen des Mathe-Kerns. Kennt weder Airflow noch dbt."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CrossEdge:
    """Kante von `src` im Lauf k - periods auf `dst` im Lauf k."""

    src: str
    dst: str
    periods: int = 1


@dataclass(frozen=True)
class Pipeline:
    durations: dict[str, float]
    intra: list[tuple[str, str]]
    cross: list[CrossEdge]

    def __post_init__(self) -> None:
        for task, duration in self.durations.items():
            if duration < 0:
                raise ValueError(f"negative Dauer fuer Task {task!r}: {duration}")
        for src, dst in self.intra:
            self._require_task(src)
            self._require_task(dst)
        for edge in self.cross:
            self._require_task(edge.src)
            self._require_task(edge.dst)
            if edge.periods < 1:
                raise ValueError(
                    f"periods muss >= 1 sein, Kante {edge.src} -> {edge.dst}: {edge.periods}"
                )
        toposort(self.tasks, self.intra)

    def _require_task(self, task: str) -> None:
        if task not in self.durations:
            raise ValueError(f"unbekannter Task in Kante: {task!r}")

    @property
    def tasks(self) -> list[str]:
        return list(self.durations)

    def predecessors(self) -> dict[str, list[str]]:
        preds: dict[str, list[str]] = {task: [] for task in self.durations}
        for src, dst in self.intra:
            preds[dst].append(src)
        return preds

    def successors(self) -> dict[str, list[str]]:
        succs: dict[str, list[str]] = {task: [] for task in self.durations}
        for src, dst in self.intra:
            succs[src].append(dst)
        return succs


def toposort(tasks: list[str], intra: list[tuple[str, str]]) -> list[str]:
    """Topologische Ordnung des Intra-Run-DAG. Wirft bei einem Zyklus."""
    preds: dict[str, list[str]] = {task: [] for task in tasks}
    for src, dst in intra:
        preds[dst].append(src)

    order: list[str] = []
    done: set[str] = set()
    active: set[str] = set()

    def visit(task: str) -> None:
        if task in done:
            return
        if task in active:
            raise ValueError(
                f"Intra-Run-Graph ist nicht azyklisch, Task {task!r} liegt auf einem Kreis"
            )
        active.add(task)
        for pred in preds[task]:
            visit(pred)
        active.discard(task)
        done.add(task)
        order.append(task)

    for task in tasks:
        visit(task)
    return order
