"""ADR-015: DAG-Konstruktoren, die ein Repo selbst definiert.

Professionelle Airflow-Umgebungen erzeugen ihre DAGs selten ueber `DAG(...)` im DAG-File.
Sie kapseln: Wikimedia schreibt `with create_easy_dag(...)`, dahinter steht eine Methode, die
ein `DAG(...)` zurueckgibt. Ein Scanner, der nur `DAG(...)` und `@dag` kennt, sieht in solchen
Repos fast nichts, und das ist genau die Zielgruppe, um die es geht (wiki/log.md, 005).

Die Regel bleibt eng, weil ein Falsch-Positiv teurer ist als ein Falsch-Negativ:

1. Konstruktor ist eine Funktion oder Methode, deren **eigener** Rumpf ein `DAG(...)`
   zurueckgibt. Ein `return` in einer verschachtelten Funktion zaehlt nicht fuer die aeussere.
2. Dazu kommen Modul-Aliase auf einen Konstruktor: `EasyDAG = EasyDAGFactory(...).create_easy_dag`
   bindet denselben Konstruktor unter neuem Namen.
3. Keine Transitivitaet. Eine Funktion, die ihrerseits einen Konstruktor aufruft und dessen
   Ergebnis weiterreicht (`build_dag()` bei Wikimedia), wird **nicht** zum Konstruktor
   befoerdert. Der DAG entsteht dort, wo der Konstruktor aufgerufen wird, und Schedule wie
   `default_args` stehen genau an dieser Stelle. Wuerde man den Aufrufer befoerdern, laege
   der Fund an der Aufrufstelle, wo beides fehlt.

Punkt 3 kostet uns die `dag_id`, wenn sie erst der Aufrufer einsetzt. Ein Fund ohne `dag_id`
ist ehrlicher als ein Fund ohne Schedule.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from scanner.schedule import call_name

DAG_NAMES = frozenset({"DAG"})


@dataclass
class ConstructorScan:
    """Was ein File zu den Konstruktoren des Repos beitraegt."""

    direct: set[str] = field(default_factory=set)
    aliases: list[tuple[str, str]] = field(default_factory=list)  # (neuer Name, Quelle)


def _own_returns(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.expr]:
    """Rueckgabe-Ausdruecke dieser Funktion, ohne die verschachtelter Funktionen."""
    found: list[ast.expr] = []
    stack: list[ast.AST] = list(ast.iter_child_nodes(node))
    while stack:
        current = stack.pop()
        if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda):
            continue
        if isinstance(current, ast.Return) and current.value is not None:
            found.append(current.value)
        stack.extend(ast.iter_child_nodes(current))
    return found


def scan_file(tree: ast.Module) -> ConstructorScan:
    result = ConstructorScan()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if any(
            isinstance(value, ast.Call) and call_name(value) in DAG_NAMES
            for value in _own_returns(node)
        ):
            result.direct.add(node.name)

    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign):
            continue
        value = stmt.value
        source: str | None = None
        if isinstance(value, ast.Attribute):
            source = value.attr  # EasyDAGFactory(...).create_easy_dag
        elif isinstance(value, ast.Name):
            source = value.id  # my_dag = create_easy_dag
        if source is None:
            continue
        for target in stmt.targets:
            if isinstance(target, ast.Name):
                result.aliases.append((target.id, source))
    return result


def resolve(scans: list[ConstructorScan]) -> frozenset[str]:
    """Repo-weite Konstruktornamen. Aliase werden bis zum Fixpunkt aufgeloest."""
    names = {name for scan in scans for name in scan.direct}
    edges = [alias for scan in scans for alias in scan.aliases]
    while True:
        grown = {new for new, source in edges if source in names} - names
        if not grown:
            return frozenset(names)
        names |= grown


def definition_spans(tree: ast.Module, names: frozenset[str]) -> list[tuple[int, int]]:
    """Zeilen-Spannen der Konstruktor-Definitionen im File.

    Das `DAG(...)` **in** einem Konstruktor ist die Schablone, nicht der DAG. Es zaehlt nicht,
    sonst haette ein Repo einen DAG mehr, als es Aufrufstellen gibt.
    """
    spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name in names
            and node.end_lineno is not None
        ):
            spans.append((node.lineno, node.end_lineno))
    return spans
