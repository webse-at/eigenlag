import pytest

from eigenlag.model import CrossEdge, Pipeline, toposort


def test_valid_pipeline_keeps_its_fields() -> None:
    pipeline = Pipeline(
        durations={"a": 1.0, "b": 2.0},
        intra=[("a", "b")],
        cross=[CrossEdge("b", "a")],
    )
    assert pipeline.tasks == ["a", "b"]
    assert pipeline.predecessors() == {"a": [], "b": ["a"]}
    assert pipeline.successors() == {"a": ["b"], "b": []}
    assert pipeline.cross[0].periods == 1


def test_unknown_task_in_intra_edge_raises() -> None:
    with pytest.raises(ValueError, match="unbekannter Task"):
        Pipeline(durations={"a": 1.0}, intra=[("a", "ghost")], cross=[])


def test_unknown_task_in_cross_edge_raises() -> None:
    with pytest.raises(ValueError, match="unbekannter Task"):
        Pipeline(durations={"a": 1.0}, intra=[], cross=[CrossEdge("ghost", "a")])


def test_negative_duration_raises() -> None:
    with pytest.raises(ValueError, match="negative Dauer"):
        Pipeline(durations={"a": -0.1}, intra=[], cross=[])


def test_periods_below_one_raises() -> None:
    with pytest.raises(ValueError, match="periods"):
        Pipeline(durations={"a": 1.0}, intra=[], cross=[CrossEdge("a", "a", periods=0)])


def test_cyclic_intra_graph_raises() -> None:
    with pytest.raises(ValueError, match="azyklisch"):
        Pipeline(durations={"a": 1.0, "b": 1.0}, intra=[("a", "b"), ("b", "a")], cross=[])


def test_toposort_puts_predecessors_first() -> None:
    order = toposort(["c", "a", "b"], [("a", "b"), ("b", "c")])
    assert order.index("a") < order.index("b") < order.index("c")
