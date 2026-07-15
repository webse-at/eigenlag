"""Tests-zuerst fuer Monte Carlo (Spec 009, Vorentscheidungen 2 bis 4).

Der Pfadwechsel-Test ist der Kern: die Kantengewichte der kondensierten Matrix sind
laengste Pfade und haengen von den gezogenen Dauern ab. Wer die Matrix einmal baut
und nur Lambda neu rechnet, rechnet falsch — bei der Fixture unten konkurrieren zwei
Intra-Pfade um den laengsten, und nur eine Kondensation pro Sample sieht den Wechsel.

Nachrechnung der Fixture: p ~ Lognormal(mu=ln 90, sigma=(ln 200 - ln 90)/1.6449),
q konstant 100. Lambda je Sample = a + max(p, q) + z. P(p <= 100) = 0.586, also ist
das Sample-Median exakt der q-Pfad (a + 100 + z); das 95. Perzentil von max(p, q)
ist das 95. Perzentil von p, per Konstruktion 200.
"""

from __future__ import annotations

import pytest

from eigenlag.durations import TaskStats, assume
from eigenlag.model import CrossEdge, Pipeline
from eigenlag.montecarlo import run

Z_95 = 1.6449


def constant(seconds: float) -> TaskStats:
    return TaskStats(p50=seconds, p95=seconds, mean=seconds, n=12, operator=None)


def wettbewerb() -> tuple[Pipeline, dict[str, TaskStats]]:
    """a -> p -> z und a -> q -> z, Cross-Kante z(k-1) -> a(k)."""
    stats = {
        "a": constant(10.0),
        "p": TaskStats(p50=90.0, p95=200.0, mean=105.0, n=40, operator=None),
        "q": constant(100.0),
        "z": constant(5.0),
    }
    pipeline = Pipeline(
        durations={"a": 10.0, "p": 105.0, "q": 100.0, "z": 5.0},
        intra=[("a", "p"), ("a", "q"), ("p", "z"), ("q", "z")],
        cross=[CrossEdge("z", "a", 1)],
    )
    return pipeline, stats


def test_pfadwechsel_bei_extremen_samples_wechselt_der_laengste_pfad() -> None:
    pipeline, stats = wettbewerb()
    result = run(pipeline, stats, samples=1000)
    assert result is not None
    # Median: mehr als die Haelfte der Samples nimmt den q-Pfad (a + 100 + z).
    assert result.lam_p50 == pytest.approx(115.0, abs=2.0)
    # p95: der p-Pfad gewinnt in den schlechten Wochen, per Konstruktion bei 200.
    # Eine einmal gebaute Matrix bliebe beim q-Pfad haengen und zeigte 115.
    assert result.lam_p95 == pytest.approx(10.0 + 200.0 + 5.0, rel=0.10)
    assert result.lam_p95 > 150.0


def test_seed_default_ist_fest_derselbe_aufruf_liefert_dieselben_zahlen() -> None:
    pipeline, stats = wettbewerb()
    first = run(pipeline, stats, samples=200)
    second = run(pipeline, stats, samples=200)
    assert first is not None and second is not None
    assert first.lam_p50 == second.lam_p50
    assert first.lam_p95 == second.lam_p95


def test_anderer_seed_liefert_andere_zahlen() -> None:
    pipeline, stats = wettbewerb()
    first = run(pipeline, stats, samples=200, seed=1)
    second = run(pipeline, stats, samples=200, seed=2)
    assert first is not None and second is not None
    assert (first.lam_p50, first.lam_p95) != (second.lam_p50, second.lam_p95)


def test_tasks_ohne_varianz_basis_werden_konstant_gesampelt() -> None:
    # Alle Tasks ohne belastbare Streuung (n=0 bzw. n<5): jedes Sample identisch,
    # Lambda-Verteilung kollabiert auf den Punktwert der Pipeline.
    pipeline = Pipeline(
        durations={"a": 60.0, "b": 30.0},
        intra=[("a", "b")],
        cross=[CrossEdge("b", "a", 1)],
    )
    stats = {"a": assume(60.0), "b": TaskStats(p50=30.0, p95=99.0, mean=30.0, n=3, operator=None)}
    result = run(pipeline, stats, samples=50, period=80.0)
    assert result is not None
    assert result.lam_p50 == result.lam_p95 == 90.0
    assert set(result.deterministic_tasks) == {"a", "b"}
    assert result.share_above_period == 1.0


def test_anteil_ueber_takt_zwischen_p50_und_p95() -> None:
    pipeline, stats = wettbewerb()
    # T = 150: stabil im Median (115), instabil in schlechten Wochen (215).
    result = run(pipeline, stats, samples=1000, period=150.0)
    assert result is not None
    assert result.share_above_period is not None
    assert 0.05 < result.share_above_period < 0.5


def test_ohne_kreis_kein_monte_carlo() -> None:
    pipeline = Pipeline(durations={"a": 1.0, "b": 2.0}, intra=[("a", "b")], cross=[])
    assert run(pipeline, {"a": constant(1.0), "b": constant(2.0)}, samples=10) is None
