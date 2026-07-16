"""Tests des Mathe-Kerns.

Die Referenz-Fixture (DUR, INTRA, CROSS aus `wiki/maxplus_pipeline.py`) lebt seit
Spec 013 in `eigenlag/demo.py`, weil `eigenlag demo` sie ausliefert; sie steht nur
dort (Single Source), die Pins bleiben hier. Der Prototyp wird nicht importiert,
er ist ein Skript mit Seiteneffekten beim Import. Alle gepinnten Werte stammen
aus seinem Lauf vom 2026-07-13 (ADR-001).
"""

from __future__ import annotations

import pytest

from eigenlag.demo import CROSS, DUR, INTRA, demo
from eigenlag.maxplus import (
    condense,
    critical_path,
    drift,
    howard,
    karp,
    simulate,
)
from eigenlag.model import CrossEdge, Pipeline


def lam_of(pipeline: Pipeline) -> float | None:
    graph, _ = condense(pipeline)
    return karp(graph)


# --- Referenz-Pins aus dem Prototyp ------------------------------------------


def test_lambda_of_demo_pipeline_is_4_40() -> None:
    assert lam_of(demo()) == pytest.approx(4.40)


def test_condensed_graph_spans_only_cross_source_nodes() -> None:
    graph, _ = condense(demo())
    assert set(graph.nodes) == {"core", "retrain", "monitor"}


def test_critical_cycle_is_the_monitor_self_loop() -> None:
    result = howard(condense(demo())[0])
    assert result is not None
    lam, cycle = result
    assert lam == pytest.approx(4.40)
    assert [edge.src for edge in cycle] == ["monitor"]
    assert [edge.dst for edge in cycle] == ["monitor"]


def test_critical_cycle_segment_resolves_to_the_intra_path() -> None:
    graph, paths = condense(demo())
    result = howard(graph)
    assert result is not None
    _, cycle = result
    edge = cycle[0]
    assert paths[(edge.src, edge.dst, edge.periods)] == (
        "core",
        "features",
        "retrain",
        "score",
        "monitor",
    )


def test_critical_path_of_a_single_run_is_5_5() -> None:
    length, path = critical_path(demo())
    assert length == pytest.approx(5.5)
    assert path == ["ingest", "dq", "core", "features", "retrain", "score", "reports"]


def test_drift_at_target_period_3_is_1_40_per_run() -> None:
    lam = lam_of(demo())
    assert lam is not None
    assert drift(lam, 3.0) == pytest.approx(1.40)


def test_whatif_halving_retrain_lowers_lambda_to_3_60() -> None:
    assert lam_of(demo(durations={"retrain": 0.8})) == pytest.approx(3.60)


def test_whatif_dropping_monitor_to_core_lowers_lambda_to_2_50() -> None:
    without_gate = [e for e in CROSS if (e.src, e.dst) != ("monitor", "core")]
    assert lam_of(demo(cross=without_gate)) == pytest.approx(2.50)


def test_whatif_halving_core_lowers_lambda_to_3_85() -> None:
    assert lam_of(demo(durations={"core": 0.55})) == pytest.approx(3.85)


# --- Karp gegen Howard (ADR-003) ---------------------------------------------

FIXTURES = {
    "demo": demo(),
    "retrain_halved": demo(durations={"retrain": 0.8}),
    "no_gate": demo(cross=[e for e in CROSS if (e.src, e.dst) != ("monitor", "core")]),
    "core_halved": demo(durations={"core": 0.55}),
    "self_loop": Pipeline(durations={"a": 2.5}, intra=[], cross=[CrossEdge("a", "a")]),
    "two_periods": Pipeline(durations={"a": 2.5}, intra=[], cross=[CrossEdge("a", "a", periods=2)]),
    "disjoint": Pipeline(
        durations={"a": 3.0, "b": 1.0},
        intra=[],
        cross=[CrossEdge("a", "a"), CrossEdge("b", "b")],
    ),
    "mixed_periods": Pipeline(
        durations={"a": 1.0, "b": 2.0, "c": 3.0},
        intra=[],
        cross=[
            CrossEdge("a", "b", periods=1),
            CrossEdge("b", "c", periods=2),
            CrossEdge("c", "a", periods=1),
        ],
    ),
}


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_karp_and_howard_agree(name: str) -> None:
    graph, _ = condense(FIXTURES[name])
    by_karp = karp(graph)
    by_howard = howard(graph)
    assert by_karp is not None
    assert by_howard is not None
    assert by_howard[0] == pytest.approx(by_karp)


# --- Simulation gegen Analytik -----------------------------------------------


def test_simulated_drift_converges_to_lambda_minus_t() -> None:
    pipeline = demo()
    lam = lam_of(pipeline)
    assert lam is not None
    target = 3.0
    latencies = simulate(pipeline, target, 20)
    measured = (latencies[-1] - latencies[-6]) / 5
    assert measured == pytest.approx(drift(lam, target))
    assert measured == pytest.approx(1.40)


def test_stable_schedule_does_not_drift() -> None:
    pipeline = demo()
    lam = lam_of(pipeline)
    assert lam is not None
    latencies = simulate(pipeline, lam + 1.0, 20)
    assert latencies[-1] == pytest.approx(latencies[-6])


def test_first_run_latency_is_the_critical_path() -> None:
    latencies = simulate(demo(), 3.0, 3)
    assert latencies[0] == pytest.approx(5.5)


# --- Edge-Cases ---------------------------------------------------------------


def test_without_cross_edges_lambda_is_none_not_zero() -> None:
    pipeline = Pipeline(durations=dict(DUR), intra=list(INTRA), cross=[])
    graph, paths = condense(pipeline)
    assert graph.nodes == ()
    assert paths == {}
    assert karp(graph) is None
    assert howard(graph) is None


def test_cross_edge_without_return_path_has_no_cycle() -> None:
    pipeline = Pipeline(
        durations={"a": 1.0, "b": 2.0},
        intra=[("a", "b")],
        cross=[CrossEdge("a", "b")],
    )
    graph, _ = condense(pipeline)
    assert karp(graph) is None
    assert howard(graph) is None


def test_single_self_loop_lambda_is_the_task_duration() -> None:
    pipeline = FIXTURES["self_loop"]
    assert lam_of(pipeline) == pytest.approx(2.5)


def test_two_period_self_loop_halves_lambda() -> None:
    pipeline = FIXTURES["two_periods"]
    assert lam_of(pipeline) == pytest.approx(1.25)
    result = howard(condense(pipeline)[0])
    assert result is not None
    lam, cycle = result
    assert lam == pytest.approx(1.25)
    assert [edge.periods for edge in cycle] == [2]


def test_two_period_loop_is_stable_at_the_halved_period() -> None:
    pipeline = FIXTURES["two_periods"]
    latencies = simulate(pipeline, 1.25, 20)
    assert latencies[-1] == pytest.approx(latencies[-6])


def test_disjoint_cycles_take_the_maximum_and_report_the_larger_one() -> None:
    pipeline = FIXTURES["disjoint"]
    assert lam_of(pipeline) == pytest.approx(3.0)
    result = howard(condense(pipeline)[0])
    assert result is not None
    lam, cycle = result
    assert lam == pytest.approx(3.0)
    assert [edge.src for edge in cycle] == ["a"]


def test_mixed_periods_cycle_mean_divides_by_the_sum_of_periods() -> None:
    # Kreis a -> b -> c -> a mit Gewichten 2.0 + 3.0 + 1.0 = 6.0 und
    # periods 1 + 2 + 1 = 4. Zyklusmittel 6.0 / 4 = 1.5, nicht 6.0 / 3 = 2.0.
    pipeline = FIXTURES["mixed_periods"]
    assert lam_of(pipeline) == pytest.approx(1.5)
    result = howard(condense(pipeline)[0])
    assert result is not None
    lam, cycle = result
    assert lam == pytest.approx(1.5)
    assert sorted(edge.src for edge in cycle) == ["a", "b", "c"]


def test_drift_is_lambda_minus_t() -> None:
    assert drift(4.4, 3.0) == pytest.approx(1.4)
    assert drift(4.4, 5.0) == pytest.approx(-0.6)
