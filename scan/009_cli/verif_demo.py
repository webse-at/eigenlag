"""Verifikation 1 (Spec 009): Demo-Pipeline als Python-Aufruf, MC-Perf-Messung."""

from __future__ import annotations

import sys
import time

sys.path.insert(0, "/mnt/data/projects/eigenlag")

from eigenlag.durations import TaskStats
from eigenlag.maxplus import condense, drift, howard
from eigenlag.maxplus_test import demo
from eigenlag.montecarlo import run
from eigenlag.report import WhatIfDropEdge, WhatIfTask, _what_if  # noqa: PLC2701
from eigenlag.analyze import Analysis

pipeline = demo()
graph, paths = condense(pipeline)
outcome = howard(graph)
assert outcome is not None
lam, cycle = outcome
print(f"Lambda           = {lam:.2f} h  (Pin: 4.40)")
print(f"Kreis kondensiert: {[(e.src, e.dst, e.periods) for e in cycle]}")
tasks: list[str] = []
for e in cycle:
    for t in paths[(e.src, e.dst, e.periods)]:
        if t not in tasks:
            tasks.append(t)
print(f"Kreis aufgeloest : {tasks}")
print(f"Drift bei T=3.0  = {drift(lam, 3.0):.2f} h/Lauf  (Pin: 1.40)")

# What-if-Szenarien des Prototyps ueber die Report-Maschinerie:
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
rows = _what_if(
    analysis,
    [WhatIfTask("retrain", 0.8), WhatIfDropEdge("monitor", "core"), WhatIfTask("core", 0.55)],
)
for row in rows:
    if row["angefragt"]:
        print(f"What-if {row['szenario']}: Lambda = {row['lambda_s']}")

# Perf-Messung (Messvorbehalt Spec 009, Vorentscheidung 2): 1000 Samples,
# alle 8 Tasks mit echtem Lognormal-Fit (p95 = 1.5 * p50, n = 40).
stats = {
    t: TaskStats(p50=d, p95=1.5 * d, mean=d, n=40, operator=None)
    for t, d in pipeline.durations.items()
}
start = time.perf_counter()
mc = run(pipeline, stats, samples=1000, period=3.0)
elapsed = time.perf_counter() - start
assert mc is not None
print(f"\nMonte Carlo 1000 Samples auf der Demo-Pipeline: {elapsed:.2f} s")
print(f"lambda_p50 = {mc.lam_p50:.2f} h, lambda_p95 = {mc.lam_p95:.2f} h")
print(f"Anteil ueber T=3.0: {mc.share_above_period:.0%}, konstant: {mc.deterministic_tasks}")
