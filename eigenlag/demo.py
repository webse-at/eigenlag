"""`eigenlag demo` (Spec 013): der volle Report der Prototyp-Pipeline, eingebaut.

Kein Netz, keine Dateien: das Pipeline-Objekt wird direkt gebaut und durch
denselben compose()/render()-Pfad geschickt, den `eigenlag analyze` benutzt.
DUR/INTRA/CROSS replizieren `wiki/maxplus_pipeline.py` (ADR-001) und stehen nur
hier; maxplus_test importiert sie, die Pins bleiben in den Tests. Dauern sind
Stunden und werden auf Sekunden skaliert (x3600), damit dur() sie als Stunden
ausgibt: lambda = 15840 s (4.4 h) bei T = 10800 s (3 h).

Die Streuung fuer Monte Carlo (p95 = 1.5 x p50) und die Stichprobe (n = 40) sind
Beispiel-Annahmen wie im abgenommenen 012-Artefakt (scan/012_plan/lauf1); der
Report deklariert beides in der Dauern-Quelle, die Kopfzeile deklariert den
ganzen Lauf als eingebautes Beispiel.
"""

from __future__ import annotations

from typing import Any

from eigenlag import montecarlo
from eigenlag.analyze import Analysis
from eigenlag.durations import TaskStats
from eigenlag.maxplus import condense, critical_path, howard
from eigenlag.messages import Lang, t
from eigenlag.model import CrossEdge, Pipeline
from eigenlag.report import compose, render

DUR = {
    "ingest": 0.7,
    "dq": 0.3,
    "core": 1.1,
    "features": 0.9,
    "retrain": 1.6,
    "score": 0.5,
    "monitor": 0.3,
    "reports": 0.4,
}
INTRA = [
    ("ingest", "dq"),
    ("dq", "core"),
    ("core", "features"),
    ("features", "retrain"),
    ("retrain", "score"),
    ("score", "monitor"),
    ("score", "reports"),
]
CROSS = [
    CrossEdge("core", "core"),
    CrossEdge("retrain", "retrain"),
    CrossEdge("retrain", "features"),
    CrossEdge("monitor", "core"),
]

PERIOD_S = 3.0 * 3600.0


def demo(
    durations: dict[str, float] | None = None,
    cross: list[CrossEdge] | None = None,
) -> Pipeline:
    merged = dict(DUR)
    merged.update(durations or {})
    return Pipeline(durations=merged, intra=list(INTRA), cross=list(cross or CROSS))


def demo_bericht() -> dict[str, Any]:
    scaled = {task: dauer * 3600.0 for task, dauer in DUR.items()}
    pipeline = demo(durations=scaled)
    graph, paths = condense(pipeline)
    outcome = howard(graph)
    assert outcome is not None  # die Demo-Pipeline hat per Konstruktion einen Kreis
    lam, cycle = outcome
    cycle_tasks: list[str] = []
    for edge in cycle:
        for task in paths[(edge.src, edge.dst, edge.periods)]:
            if task not in cycle_tasks:
                cycle_tasks.append(task)
    cp_s, cp_tasks = critical_path(pipeline)
    analysis = Analysis(
        pipeline=pipeline,
        lam=lam,
        cycle=tuple(cycle),
        cycle_tasks=tuple(cycle_tasks),
        critical_path_s=cp_s,
        critical_path_tasks=cp_tasks,
        parse_warnings=(),
        warnings=(),
    )
    stats = {
        task: TaskStats(p50=dauer, p95=1.5 * dauer, mean=dauer, n=40, operator=None)
        for task, dauer in scaled.items()
    }
    mc = montecarlo.run(pipeline, stats, samples=1000, period=PERIOD_S)
    return compose(
        pfad="built-in demo",
        dags=(),
        analysis=analysis,
        stats=stats,
        statistic="mean",
        takt_s=PERIOD_S,
        takt_quelle="built-in example (T = 3 h)",
        dauern_quelle=(
            "built-in example: prototype durations in hours, scaled to seconds;"
            " spread for Monte Carlo assumed as p95 = 1.5 x p50"
        ),
        monte_carlo=mc,
    )


def demo_text(lang: Lang) -> str:
    return f"{t(lang, 'demo_kopf')}\n\n{render(demo_bericht(), lang)}\n{t(lang, 'demo_fuss')}"
