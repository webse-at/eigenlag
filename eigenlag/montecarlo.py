"""Monte Carlo ueber die Task-Dauern (Spec 009): eine Verteilung von Lambda statt
eines Punktwerts.

Die Lognormal-Fits kommen analytisch aus den vorhandenen Aggregaten: mu = ln(p50),
sigma = (ln(p95) - ln(p50)) / 1.6449 (z-Wert der 95. Perzentile). Lognormal, weil
Laufzeiten positiv und rechtsschief sind (wiki/math.md, Abschnitt 7). Tasks ohne
belastbare Streuung (n < 5, insbesondere assume mit n = 0) gehen als Konstante ins
Sampling — eine erfundene Varianz waere eine erfundene p95.

Die Kondensation laeuft pro Sample neu: die Kantengewichte der kondensierten Matrix
sind laengste Pfade und haengen von den gezogenen Dauern ab. Bei anderen Dauern kann
ein anderer Pfad der laengste sein (Pfadwechsel-Test in montecarlo_test.py).

Alles stdlib (random.lognormvariate, statistics.quantiles): der Kern bleibt
abhaengigkeitsfrei, solange 1000 Samples auf der Demo-Pipeline unter 5 s bleiben
(Messvorbehalt Spec 009, Zahl im Session-Log).
"""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Mapping
from dataclasses import dataclass

from eigenlag.durations import MIN_SAMPLE, TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.model import Pipeline

Z_95 = 1.6449

DEFAULT_SAMPLES = 1000
DEFAULT_SEED = 20260715  # fest: derselbe Aufruf liefert dieselben Zahlen


@dataclass(frozen=True)
class Fit:
    mu: float
    sigma: float


@dataclass(frozen=True)
class MonteCarloResult:
    lam_p50: float
    lam_p95: float
    share_above_period: float | None  # Anteil Samples mit Lambda > T; None ohne T
    samples: int
    seed: int
    deterministic_tasks: tuple[str, ...]  # konstant gesampelt: keine Varianz-Basis


def _fit(stats: TaskStats) -> Fit | None:
    """Analytischer Lognormal-Fit aus p50/p95, None wenn keine Basis dafuer da ist."""
    if stats.n < MIN_SAMPLE or stats.p50 <= 0 or stats.p95 < stats.p50:
        return None
    return Fit(mu=math.log(stats.p50), sigma=(math.log(stats.p95) - math.log(stats.p50)) / Z_95)


def run(
    pipeline: Pipeline,
    stats: Mapping[str, TaskStats],
    samples: int = DEFAULT_SAMPLES,
    seed: int = DEFAULT_SEED,
    period: float | None = None,
) -> MonteCarloResult | None:
    """Zieht je Sample Dauern, kondensiert neu, rechnet Howard, sammelt Lambda.

    None, wenn die Pipeline keinen Kreis hat (dann gibt es kein Lambda, das streuen
    koennte) oder samples == 0. Tasks ohne Fit behalten ihren Punktwert aus
    `pipeline.durations` — dort steckt bereits der aufgeloeste Assume-Fallback.
    """
    if samples <= 0 or howard(condense(pipeline)[0]) is None:
        return None

    fits: dict[str, Fit] = {}
    deterministic: list[str] = []
    for task in pipeline.tasks:
        found = stats.get(task)
        fit = _fit(found) if found is not None else None
        if fit is None:
            deterministic.append(task)
        else:
            fits[task] = fit

    rng = random.Random(seed)
    lams: list[float] = []
    for _ in range(samples):
        durations = dict(pipeline.durations)
        for task, fit in fits.items():
            durations[task] = rng.lognormvariate(fit.mu, fit.sigma)
        sampled = Pipeline(durations=durations, intra=pipeline.intra, cross=pipeline.cross)
        outcome = howard(condense(sampled)[0])
        assert outcome is not None  # Kreis ist Struktur, nicht Dauer: oben geprueft
        lams.append(outcome[0])

    if len(lams) == 1:
        p50 = p95 = lams[0]
    else:
        cuts = statistics.quantiles(lams, n=100, method="inclusive")
        p50, p95 = cuts[49], cuts[94]
    share = sum(1 for lam in lams if lam > period) / len(lams) if period is not None else None
    return MonteCarloResult(
        lam_p50=p50,
        lam_p95=p95,
        share_above_period=share,
        samples=samples,
        seed=seed,
        deterministic_tasks=tuple(sorted(deterministic)),
    )
