"""Dritt-Meinung gegen Karp und Howard: Brute-Force ueber alle einfachen Kreise.

Karp und Howard stammen aus derselben Session. Ihre Uebereinstimmung (ADR-003)
schliesst einen gemeinsamen Denkfehler nicht aus, etwa in der Behandlung des
Perioden-Versatzes (ADR-006), die in beide Verfahren getrennt eingeflossen ist.

Dieser Test stellt ein drittes, absichtlich stumpfes Verfahren daneben: alle
einfachen Kreise aufzaehlen, je Kreis Summe der Gewichte durch Summe der periods,
Maximum bilden. Das ist exponentiell und fuer den Produktiv-Pfad unbrauchbar
(CLAUDE.md, Anti-Pattern 4), aber als Referenz auf kleinen Graphen unschlagbar,
weil es die Definition aus ADR-006 direkt abbildet und keinen Algorithmus enthaelt,
in dem sich ein Fehler verstecken koennte.

Erzeugt vom Orchestrator bei der Abnahme von Session 004.
"""

from __future__ import annotations

import itertools
import random

from eigenlag.maxplus import CondensedEdge, CondensedGraph, howard, karp

TOL = 1e-7


def max_cycle_mean_bruteforce(graph: CondensedGraph) -> float | None:
    """Maximales Zyklusmittel durch Aufzaehlung. Referenz, kein Produktiv-Code."""
    out: dict[str, list[CondensedEdge]] = {}
    for edge in graph.edges:
        out.setdefault(edge.src, []).append(edge)

    best: float | None = None
    for size in range(1, len(graph.nodes) + 1):
        for combo in itertools.permutations(graph.nodes, size):
            legs: list[list[CondensedEdge]] = []
            for i in range(size):
                src, dst = combo[i], combo[(i + 1) % size]
                parallel = [e for e in out.get(src, []) if e.dst == dst]
                if not parallel:
                    break
                legs.append(parallel)
            else:
                for choice in itertools.product(*legs):
                    weight = sum(e.weight for e in choice)
                    periods = sum(e.periods for e in choice)
                    mean = weight / periods
                    if best is None or mean > best:
                        best = mean
    return best


def _random_graph(rng: random.Random) -> CondensedGraph:
    nodes = tuple(f"n{i}" for i in range(rng.randint(1, 5)))
    edges = tuple(
        CondensedEdge(
            src=src,
            dst=dst,
            weight=round(rng.uniform(0.1, 5.0), 2),
            periods=rng.choice([1, 1, 1, 2, 3]),
        )
        for src in nodes
        for dst in nodes
        if rng.random() < 0.45
    )
    return CondensedGraph(nodes=nodes, edges=edges)


def _same(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return abs(left - right) < TOL


def test_karp_und_howard_treffen_die_bruteforce_referenz() -> None:
    """Alle drei Verfahren liefern dasselbe Lambda, auch bei Versatz > 1."""
    rng = random.Random(7)
    acyclic = 0

    for _ in range(1000):
        graph = _random_graph(rng)
        reference = max_cycle_mean_bruteforce(graph)
        from_karp = karp(graph)
        result = howard(graph)
        from_howard = None if result is None else result[0]

        assert _same(reference, from_karp), (graph, reference, from_karp)
        assert _same(reference, from_howard), (graph, reference, from_howard)

        if reference is None:
            acyclic += 1

    # Ohne kreislose Graphen waere der None-Pfad aus ADR-007 ungeprueft.
    assert acyclic > 50, f"nur {acyclic} kreislose Graphen, Stichprobe deckt ADR-007 nicht ab"
