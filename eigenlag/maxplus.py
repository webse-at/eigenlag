"""Kondensation, Karp, Howard, Drift.

Der Kern rechnet in der Max-Plus-Algebra: Warten auf den langsamsten Vorgaenger
ist ein Maximum, das Anhaengen der Laufzeit eine Addition. Der Max-Plus-Eigenwert
der Cross-Run-Matrix ist das maximale Zyklusmittel des kondensierten Graphen und
damit die minimale nachhaltige Taktzeit. Siehe wiki/math.md.

Der Perioden-Versatz einer Cross-Kante geht als Kantenlaenge ins Zyklusmittel ein:

    Zyklusmittel = Summe der Gewichte / Summe der periods

Eine Kante mit Versatz n ist eine Verzoegerung um n Perioden. Die uebliche
Zustandserweiterung ersetzt sie durch eine Kette aus n Kanten der Laenge 1, von
denen die erste das Gewicht traegt und die restlichen null. Das Zyklusmittel des
erweiterten Graphen ist genau der obige Quotient. Fuer periods == 1 ueberall
faellt er auf das gewohnte Summe/Kantenzahl zurueck.
"""

from __future__ import annotations

from dataclasses import dataclass

from eigenlag.model import Pipeline, toposort

NEG = float("-inf")
TOL = 1e-9


@dataclass(frozen=True)
class CondensedEdge:
    """Kante der Cross-Run-Matrix: `src` im Lauf k - periods speist `dst` im Lauf k."""

    src: str
    dst: str
    weight: float
    periods: int


@dataclass(frozen=True)
class CondensedGraph:
    nodes: tuple[str, ...]
    edges: tuple[CondensedEdge, ...]


PathMap = dict[tuple[str, str, int], tuple[str, ...]]


def longest_intra_from(
    pipeline: Pipeline, start: str
) -> tuple[dict[str, float], dict[str, str | None]]:
    """Laengster Intra-Pfad von `start` zu jedem Task, Gewicht inklusive beider Enden."""
    order = toposort(pipeline.tasks, pipeline.intra)
    succs = pipeline.successors()

    dist: dict[str, float] = {task: NEG for task in pipeline.tasks}
    parent: dict[str, str | None] = {task: None for task in pipeline.tasks}
    dist[start] = pipeline.durations[start]

    for task in order:
        if dist[task] == NEG:
            continue
        for succ in succs[task]:
            candidate = dist[task] + pipeline.durations[succ]
            if candidate > dist[succ]:
                dist[succ] = candidate
                parent[succ] = task
    return dist, parent


def _path_to(parent: dict[str, str | None], task: str) -> tuple[str, ...]:
    path = [task]
    node = parent[task]
    while node is not None:
        path.append(node)
        node = parent[node]
    return tuple(reversed(path))


def critical_path(pipeline: Pipeline) -> tuple[float, list[str]]:
    """Laengster Pfad eines Einzellaufs: die Latenz, die heutige Tools zeigen."""
    order = toposort(pipeline.tasks, pipeline.intra)
    preds = pipeline.predecessors()

    dist: dict[str, float] = {}
    parent: dict[str, str | None] = {}
    for task in order:
        best_pred: str | None = None
        best = 0.0
        for pred in preds[task]:
            if dist[pred] > best:
                best = dist[pred]
                best_pred = pred
        dist[task] = best + pipeline.durations[task]
        parent[task] = best_pred

    end = max(dist, key=lambda task: dist[task])
    return dist[end], list(_path_to(parent, end))


def condense(pipeline: Pipeline) -> tuple[CondensedGraph, PathMap]:
    """Kondensiert die Pipeline auf ihre Cross-Run-Quellknoten.

    Zeilen und Spalten spannen nur ueber Tasks mit ausgehender Cross-Kante: ein
    Task ohne solche Kante kann auf keinem Kreis ueber die Zeitachse liegen.
    Kanten mit verschiedenem Versatz bleiben getrennt, weil keine die andere
    dominiert (ein hoeheres Gewicht bei doppeltem Versatz kann ein kleineres
    Zyklusmittel ergeben).
    """
    sources = [task for task in pipeline.tasks if any(e.src == task for e in pipeline.cross)]
    weights: dict[tuple[str, str, int], float] = {}
    paths: PathMap = {}

    for edge in pipeline.cross:
        dist, parent = longest_intra_from(pipeline, edge.dst)
        for target in sources:
            if dist[target] == NEG:
                continue
            key = (edge.src, target, edge.periods)
            if dist[target] > weights.get(key, NEG):
                weights[key] = dist[target]
                paths[key] = _path_to(parent, target)

    edges = tuple(
        CondensedEdge(src=src, dst=dst, weight=weight, periods=periods)
        for (src, dst, periods), weight in weights.items()
    )
    return CondensedGraph(nodes=tuple(sources), edges=edges), paths


def _expand(graph: CondensedGraph) -> tuple[int, list[tuple[int, int, float]]]:
    """Zustandserweiterung: eine Kante mit Versatz n wird zu n Kanten der Laenge 1.

    Die Zwischenknoten tragen kein Gewicht, sie zaehlen nur Perioden. Danach ist
    jede Kante genau eine Periode lang, und Karp ist ohne Anpassung anwendbar.
    """
    index = {node: i for i, node in enumerate(graph.nodes)}
    count = len(graph.nodes)
    edges: list[tuple[int, int, float]] = []

    for edge in graph.edges:
        source = index[edge.src]
        for step in range(edge.periods - 1):
            dummy = count
            count += 1
            edges.append((source, dummy, edge.weight if step == 0 else 0.0))
            source = dummy
        edges.append((source, index[edge.dst], edge.weight if edge.periods == 1 else 0.0))
    return count, edges


def karp(graph: CondensedGraph) -> float | None:
    """Maximales Zyklusmittel in O(V*E). Liefert None, wenn es keinen Kreis gibt.

    Kein Kreis heisst keine Rekurrenz und damit kein Lambda. Eine Null waere eine
    Falschaussage, weil sie als Entwarnung gelesen wird (wiki/math.md, Abschnitt 8).
    """
    size, edges = _expand(graph)
    if size == 0:
        return None

    preds: list[list[tuple[int, float]]] = [[] for _ in range(size)]
    for src, dst, weight in edges:
        preds[dst].append((src, weight))

    # D[k][v] = schwerster Weg aus genau k Kanten, der in v endet.
    # D[0][v] = 0 fuer alle v entspricht einer virtuellen Quelle mit Null-Kanten
    # in jeden Knoten. Sie erzeugt keinen Kreis und macht jeden Knoten erreichbar.
    table: list[list[float]] = [[NEG] * size for _ in range(size + 1)]
    table[0] = [0.0] * size
    for step in range(1, size + 1):
        previous = table[step - 1]
        current = table[step]
        for dst in range(size):
            best = NEG
            for src, weight in preds[dst]:
                if previous[src] > NEG:
                    best = max(best, previous[src] + weight)
            current[dst] = best

    lam = NEG
    for node in range(size):
        final = table[size][node]
        if final == NEG:
            continue
        lam = max(
            lam,
            min(
                (final - table[step][node]) / (size - step)
                for step in range(size)
                if table[step][node] > NEG
            ),
        )
    return None if lam == NEG else lam


def _prune_to_cyclic_core(graph: CondensedGraph) -> dict[str, list[CondensedEdge]]:
    """Knoten ohne ausgehende Kante entfernen, bis ein Fixpunkt erreicht ist.

    Ein solcher Knoten kann auf keinem Kreis liegen. Was uebrig bleibt, hat
    ueberall Ausgrad >= 1, und genau das braucht Howard fuer eine Policy.
    """
    out: dict[str, list[CondensedEdge]] = {node: [] for node in graph.nodes}
    for edge in graph.edges:
        out[edge.src].append(edge)

    while True:
        dead = {node for node, edges in out.items() if not edges}
        if not dead:
            return out
        out = {
            node: [edge for edge in edges if edge.dst not in dead]
            for node, edges in out.items()
            if node not in dead
        }


def _evaluate(
    out: dict[str, list[CondensedEdge]], policy: dict[str, CondensedEdge]
) -> tuple[dict[str, float], dict[str, float]]:
    """Zyklusmittel eta und Bias v der aktuellen Policy.

    Jeder Knoten hat unter der Policy genau eine ausgehende Kante, der Policy-Graph
    ist also funktional: jeder Weg laeuft in genau einen Kreis. eta ist das
    Zyklusmittel dieses Kreises, v das Potenzial relativ dazu.
    """
    eta: dict[str, float] = {}
    bias: dict[str, float] = {}

    for start in out:
        if start in eta:
            continue
        walk: list[str] = []
        seen: dict[str, int] = {}
        node = start
        while node not in eta and node not in seen:
            seen[node] = len(walk)
            walk.append(node)
            node = policy[node].dst

        if node in seen:  # neuer Kreis, beginnt bei walk[seen[node]]
            cycle = walk[seen[node] :]
            weight = sum(policy[member].weight for member in cycle)
            periods = sum(policy[member].periods for member in cycle)
            ratio = weight / periods
            for member in cycle:
                eta[member] = ratio
            bias[cycle[0]] = 0.0
            for member in reversed(cycle[1:]):
                edge = policy[member]
                bias[member] = edge.weight - ratio * edge.periods + bias[edge.dst]
            tail = walk[: seen[node]]
        else:  # laeuft in einen bereits bewerteten Knoten
            tail = walk

        for member in reversed(tail):
            edge = policy[member]
            eta[member] = eta[edge.dst]
            bias[member] = edge.weight - eta[member] * edge.periods + bias[edge.dst]

    return eta, bias


def howard(graph: CondensedGraph) -> tuple[float, list[CondensedEdge]] | None:
    """Policy-Iteration: liefert Lambda und den kritischen Kreis in einem Durchgang.

    Der kritische Kreis ist der Kreis der finalen Policy mit maximalem
    Zyklusmittel. None, wenn der Graph keinen Kreis enthaelt.
    """
    out = _prune_to_cyclic_core(graph)
    if not out:
        return None

    policy: dict[str, CondensedEdge] = {node: edges[0] for node, edges in out.items()}
    limit = 100 * (len(out) + 1) ** 2  # Reissleine, Howard terminiert weit frueher

    for _ in range(limit):
        eta, bias = _evaluate(out, policy)
        improved = False
        for node, edges in out.items():
            best = policy[node]
            best_eta = eta[node]
            best_gain = bias[node]
            for edge in edges:
                if eta[edge.dst] > best_eta + TOL:
                    best, best_eta = edge, eta[edge.dst]
                    best_gain = edge.weight - eta[edge.dst] * edge.periods + bias[edge.dst]
                elif abs(eta[edge.dst] - best_eta) <= TOL:
                    gain = edge.weight - best_eta * edge.periods + bias[edge.dst]
                    if gain > best_gain + TOL:
                        best, best_gain = edge, gain
            if best is not policy[node]:
                policy[node] = best
                improved = True
        if not improved:
            return eta[max(eta, key=lambda node: eta[node])], _cycle_of(
                policy, max(eta, key=lambda node: eta[node])
            )

    raise RuntimeError("Howard-Policy-Iteration terminiert nicht")


def _cycle_of(policy: dict[str, CondensedEdge], start: str) -> list[CondensedEdge]:
    seen: dict[str, int] = {}
    walk: list[str] = []
    node = start
    while node not in seen:
        seen[node] = len(walk)
        walk.append(node)
        node = policy[node].dst
    return [policy[member] for member in walk[seen[node] :]]


def drift(lam: float, period: float) -> float:
    """Verspaetung, die pro Lauf hinzukommt. Negativ heisst stabil."""
    return lam - period


def simulate(pipeline: Pipeline, period: float, runs: int) -> list[float]:
    """Laufzeit-Simulation. Liefert je Lauf die Latenz: Ende des Laufs minus Release.

    Der empirische Gegencheck zur Analytik: die Latenz waechst pro Lauf um lambda - T.
    """
    order = toposort(pipeline.tasks, pipeline.intra)
    preds = pipeline.predecessors()
    history: list[dict[str, float]] = []
    latencies: list[float] = []

    for run in range(runs):
        release = run * period
        completion: dict[str, float] = {}
        for task in order:
            start = release
            for pred in preds[task]:
                start = max(start, completion[pred])
            for edge in pipeline.cross:
                if edge.dst == task and run - edge.periods >= 0:
                    start = max(start, history[run - edge.periods][edge.src])
            completion[task] = start + pipeline.durations[task]
        history.append(completion)
        latencies.append(max(completion.values()) - release)

    return latencies
