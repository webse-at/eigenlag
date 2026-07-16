"""Verifikation 1 (Spec 012): Demo-Pipeline (Prototyp-Ground-Truth) als voller
EN-Report ueber den echten compose()/render()-Pfad der CLI.

Die Demo laesst sich nicht aus einem DAG-File parsen (per-Task-Dauern in Stunden,
keine Metadaten-DB), deshalb wird das Pipeline-Objekt direkt gebaut und durch
denselben compose()/render() geschickt, den `eigenlag analyze` benutzt. Dauern
sind auf Sekunden skaliert (x3600), damit dur() sie als Stunden ausgibt:
lambda = 4.40 h, T = 3.0 h. Das ist das Marketing-Artefakt: das GPU-Upgrade
(retrain halbiert -> 3.60 h) rettet den Takt nicht, die kostenlose
Architektur-Aenderung (Quality-Gate-Kante monitor -> core entfernt -> 2.50 h) schon.
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/mnt/data/projects/eigenlag")

from eigenlag import montecarlo
from eigenlag.analyze import Analysis
from eigenlag.durations import TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.maxplus_test import DUR, demo
from eigenlag.report import compose, render

H = 3600.0
scaled = {task: dur * H for task, dur in DUR.items()}
pipeline = demo(durations=scaled)
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
stats = {
    task: TaskStats(p50=d, p95=1.5 * d, mean=d, n=40, operator=None)
    for task, d in scaled.items()
}
mc = montecarlo.run(pipeline, stats, samples=1000, period=3.0 * H)
bericht = compose(
    pfad="demo (prototype pipeline)",
    dags=(),
    analysis=analysis,
    stats=stats,
    statistic="mean",
    takt_s=3.0 * H,
    takt_quelle="prototype (T = 3.0 h)",
    dauern_quelle="prototype durations (hours, scaled to seconds)",
    monte_carlo=mc,
)
print(render(bericht, "en"))
