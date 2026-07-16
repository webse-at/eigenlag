"""Verifikation 3 (Spec 012): ein instabiler Fall, bei dem KEINE Einzel-Aktion den
Takt tragfaehig macht, die Paar-Rechnung der Top-3 aber schon.

Zwei unabhaengige, gleich schwere Selbst-Loops (zwei inkrementelle Modelle, jedes
5 h, beide ueber dem Takt T = 4 h), die in einen gemeinsamen Report-Task muenden.
lambda = 5 h. Jede Einzel-Aktion laesst den jeweils anderen Loop bindend; erst beide
Kanten zusammen entfernt loesen den Kreis ganz auf. Dauern auf Sekunden skaliert.
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/mnt/data/projects/eigenlag")

from eigenlag.analyze import Analysis
from eigenlag.durations import TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.model import CrossEdge, Pipeline
from eigenlag.report import compose, render

H = 3600.0
durations = {"model_a": 5 * H, "model_b": 5 * H, "report": 0.2 * H}
pipeline = Pipeline(
    durations=durations,
    intra=[("model_a", "report"), ("model_b", "report")],
    cross=[CrossEdge("model_a", "model_a", 1), CrossEdge("model_b", "model_b", 1)],
)
graph, paths = condense(pipeline)
outcome = howard(graph)
assert outcome is not None
lam, cycle = outcome
tasks: list[str] = []
for edge in cycle:
    for task in paths[(edge.src, edge.dst, edge.periods)]:
        if task not in tasks:
            tasks.append(task)
analysis = Analysis(
    pipeline=pipeline,
    lam=lam,
    cycle=tuple(cycle),
    cycle_tasks=tuple(tasks),
    critical_path_s=0.0,
    critical_path_tasks=[],
    parse_warnings=(),
    warnings=(),
)
stats = {t: TaskStats(p50=d, p95=1.2 * d, mean=d, n=40, operator=None) for t, d in durations.items()}
bericht = compose(
    pfad="synthetic (two co-binding incremental models)",
    dags=(),
    analysis=analysis,
    stats=stats,
    statistic="mean",
    takt_s=4.0 * H,
    takt_quelle="synthetic (T = 4.0 h)",
    dauern_quelle="synthetic durations (hours, scaled to seconds)",
    monte_carlo=None,
)
print(render(bericht, "en"))
