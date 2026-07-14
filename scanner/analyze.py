"""AST-Analyse eines geklonten Repos: DAGs finden, Signale DAG-scoped zuordnen.

Kein Regex auf Python-Quelltext. Ein `depends_on_past=True` in einem Kommentar, einem
Docstring oder einem String-Literal ist kein Signal, und nur der AST unterscheidet das
zuverlaessig (CLAUDE.md, Regel 4). Fremde Repos sind Systemgrenze: ein `SyntaxError` ist
erwartbar, wird protokolliert und bricht den Lauf nicht ab.

Signal-Definitionen: wiki/signals.md.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scanner import wrappers
from scanner.schedule import ScheduleClass, call_name, classify_node, timedelta_seconds

Json = dict[str, Any]  # Fehler-Records gehen als JSONL nach aussen, kein festes Schema.

DAG_NAMES = frozenset({"DAG"})
DECORATOR_NAMES = {"dag"}
TASK_DECORATOR = "task"
TASK_CALL_SUFFIXES = ("Operator", "Sensor")
BOOL_SIGNALS = {"depends_on_past", "wait_for_downstream"}
OFFSET_KWARGS = {"execution_delta", "execution_date_fn"}
SCHEDULE_KWARGS = ["schedule", "schedule_interval", "timetable"]

# Signal F, harte Variante: verweist auf den *erfolgreichen* Vorlauf, wartet also auf ihn.
PREV_SUCCESS = re.compile(
    r"\{\{[^{}]*\bprev_(start_date_success|data_interval_start_success|"
    r"data_interval_end_success)\b"
)
# Signal F, schwache Variante: reine Datums-Arithmetik, keine Wartesemantik.
PREV_DATE = re.compile(r"\{\{[^{}]*\bprev_(ds|ds_nodash|execution_date)\b")

# Airflow injiziert denselben Kontext ueber den Parameternamen einer Callable (ADR-013).
# `def f(prev_start_date_success, **kwargs)` wartet genauso auf den Vorlauf wie das Template.
PREV_SUCCESS_PARAMS = {
    "prev_start_date_success",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
}
PREV_DATE_PARAMS = {"prev_ds", "prev_ds_nodash", "prev_execution_date"}

STRONG_KINDS = {
    "depends_on_past",
    "wait_for_downstream",
    "external_task_sensor",
    "include_prior_dates",
    "prev_run_success",
    "dbt_incremental",
    "max_active_runs",
}
WEAK_KINDS = {"prev_run_date"}


@dataclass(frozen=True)
class Signal:
    kind: str
    file: str
    lineno: int
    source: str  # operator | default_args | template | factory
    inferred: bool = False


@dataclass
class DagFinding:
    dag_id: str | None
    file: str
    lineno: int
    schedule: ScheduleClass
    schedule_expr: str | None
    task_count: int = 0
    signals: list[Signal] = field(default_factory=list)

    @property
    def is_risk_candidate(self) -> bool:
        return self.schedule == "subdaily" and any(s.kind in STRONG_KINDS for s in self.signals)


@dataclass
class FileAnalysis:
    dags: list[DagFinding] = field(default_factory=list)
    factories: list[Signal] = field(default_factory=list)
    errors: list[Json] = field(default_factory=list)
    syntax_error: bool = False


@dataclass
class RepoAnalysis:
    repo: str
    dags: list[DagFinding] = field(default_factory=list)
    factories: list[Signal] = field(default_factory=list)
    errors: list[Json] = field(default_factory=list)
    files_parsed: int = 0
    syntax_errors: int = 0
    dag_names: frozenset[str] = frozenset()  # DAG plus die Konstruktoren des Repos (ADR-015)

    @property
    def risk_candidates(self) -> list[DagFinding]:
        return [d for d in self.dags if d.is_risk_candidate]


@dataclass
class _Scope:
    """Ein DAG im File, plus die Mittel, ihm Tasks zuzuordnen."""

    finding: DagFinding
    var: str | None
    span: tuple[int, int] | None


def _is_dag_call(node: ast.expr, dag_names: frozenset[str]) -> bool:
    return isinstance(node, ast.Call) and call_name(node) in dag_names


def _kwarg(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _kwarg_node(call: ast.Call, name: str) -> ast.keyword | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw
    return None


def _is_true(node: ast.expr | None) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def _dag_id(call: ast.Call, tree: ast.Module, fallback: str | None = None) -> str | None:
    """dag_id aus dem Aufruf. Eine Variable wird aufgeloest, wenn sie ein Modul-Literal ist."""
    node = _kwarg(call, "dag_id")
    if node is None and call.args:
        node = call.args[0]
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return _module_strings(tree).get(node.id, fallback)
    return fallback


def _schedule(call: ast.Call) -> tuple[ScheduleClass, str | None]:
    for name in SCHEDULE_KWARGS:
        node = _kwarg(call, name)
        if node is not None:
            return classify_node(node)
    # Ohne Schedule-Argument ist der Airflow-Default timedelta(days=1), also nicht sub-taeglich.
    return "daily_or_slower", None


def _module_dicts(tree: ast.Module) -> dict[str, ast.Dict]:
    found: dict[str, ast.Dict] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Dict):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    found[target.id] = stmt.value
    return found


def _module_strings(tree: ast.Module) -> dict[str, str]:
    found: dict[str, str] = {}
    for stmt in tree.body:
        if (
            isinstance(stmt, ast.Assign)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        ):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    found[target.id] = stmt.value.value
    return found


def _default_args_signals(
    call: ast.Call, tree: ast.Module, path: str, errors: list[Json]
) -> list[Signal]:
    node = _kwarg(call, "default_args")
    if node is None:
        return []
    literal = node if isinstance(node, ast.Dict) else None
    if literal is None and isinstance(node, ast.Name):
        literal = _module_dicts(tree).get(node.id)
    if literal is None:
        errors.append(
            {
                "kind": "unresolved_default_args",
                "file": path,
                "lineno": node.lineno,
                "expr": ast.unparse(node),
            }
        )
        return []

    signals = []
    for key, value in zip(literal.keys, literal.values, strict=True):
        if (
            isinstance(key, ast.Constant)
            and key.value in BOOL_SIGNALS
            and isinstance(value, ast.Constant)
            and value.value is True
        ):
            signals.append(
                Signal(kind=str(key.value), file=path, lineno=key.lineno, source="default_args")
            )
    return signals


def _collect_scopes(
    tree: ast.Module, path: str, errors: list[Json], dag_names: frozenset[str]
) -> list[_Scope]:
    scopes: list[_Scope] = []
    # Ein DAG-Aufruf im Rumpf eines Konstruktors ist dessen Schablone (ADR-015), kein DAG.
    templates = wrappers.definition_spans(tree, dag_names)

    def is_template(lineno: int) -> bool:
        return any(start <= lineno <= end for start, end in templates)

    for node in ast.walk(tree):
        if isinstance(node, ast.With | ast.AsyncWith):
            for item in node.items:
                if not _is_dag_call(item.context_expr, dag_names) or is_template(node.lineno):
                    continue
                call = item.context_expr
                assert isinstance(call, ast.Call)
                var = item.optional_vars.id if isinstance(item.optional_vars, ast.Name) else None
                scopes.append(_scope(call, tree, path, errors, var, (node.lineno, node.end_lineno)))
        elif (
            isinstance(node, ast.Assign)
            and _is_dag_call(node.value, dag_names)
            and not is_template(node.lineno)
        ):
            call = node.value
            assert isinstance(call, ast.Call)
            target = node.targets[0]
            var = target.id if isinstance(target, ast.Name) else None
            scopes.append(_scope(call, tree, path, errors, var, None))
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            for deco in node.decorator_list:
                decorated = deco if isinstance(deco, ast.Call) else None
                name = call_name(decorated) if decorated else _name_of(deco)
                if name not in DECORATOR_NAMES:
                    continue
                if decorated is None:  # `@dag` ohne Klammern: leerer Aufruf als Traeger
                    decorated = ast.Call(func=ast.Name(id="dag"), args=[], keywords=[])
                    decorated.lineno = deco.lineno
                scopes.append(
                    _scope(
                        decorated,
                        tree,
                        path,
                        errors,
                        None,
                        (node.lineno, node.end_lineno),
                        fallback_id=node.name,
                    )
                )
    return scopes


def _name_of(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _decorator_root(node: ast.expr) -> str:
    """Wurzel eines Dekorators: `task`, `task.bash`, `task(retries=1)` ergeben alle `task`."""
    current = node.func if isinstance(node, ast.Call) else node
    while isinstance(current, ast.Attribute):
        current = current.value
    return current.id if isinstance(current, ast.Name) else ""


def _is_task_call(call: ast.Call) -> bool:
    return call_name(call).endswith(TASK_CALL_SUFFIXES)


def _serialized(call: ast.Call, path: str) -> list[Signal]:
    """ADR-016: `max_active_runs=1` serialisiert die Laeufe und ist damit eine Cross-Run-Kante.

    Lauf k kann nicht beginnen, bevor Lauf k-1 fertig ist. Das ist ein Kreis ueber die
    Zeitachse, unabhaengig davon, ob ein Task auf seinen Vorlauf schaut. Nur die explizite 1
    zaehlt: Airflows Default ist groesser und laesst Laeufe nebeneinander laufen.
    """
    node = _kwarg_node(call, "max_active_runs")
    if node is None or not isinstance(node.value, ast.Constant):
        return []
    value = node.value.value
    if isinstance(value, bool) or not isinstance(value, int) or value != 1:
        return []
    return [Signal(kind="max_active_runs", file=path, lineno=node.lineno, source="dag_call")]


def _scope(
    call: ast.Call,
    tree: ast.Module,
    path: str,
    errors: list[Json],
    var: str | None,
    span: tuple[int, int | None] | None,
    fallback_id: str | None = None,
) -> _Scope:
    schedule, expr = _schedule(call)
    finding = DagFinding(
        dag_id=_dag_id(call, tree, fallback_id),
        file=path,
        lineno=call.lineno,
        schedule=schedule,
        schedule_expr=expr,
    )
    finding.signals.extend(_serialized(call, path))
    finding.signals.extend(_default_args_signals(call, tree, path, errors))
    end = span[1] if span and span[1] is not None else None
    return _Scope(
        finding=finding,
        var=var,
        span=(span[0], end) if span and end is not None else None,
    )


def _owner_of_line(lineno: int, scopes: list[_Scope]) -> _Scope | None:
    lexical = [s for s in scopes if s.span and s.span[0] <= lineno <= s.span[1]]
    if not lexical:
        return None
    return max(lexical, key=lambda s: s.span[0])  # type: ignore[index]  # span geprueft


def _owner(call: ast.Call, scopes: list[_Scope]) -> _Scope | None:
    lexical = _owner_of_line(call.lineno, scopes)
    if lexical:
        return lexical
    bound = _kwarg(call, "dag")
    if isinstance(bound, ast.Name):
        for scope in scopes:
            if scope.var == bound.id:
                return scope
    return None


def _is_zero_offset(node: ast.expr) -> bool:
    """`execution_delta=None` oder ein Versatz von null zeigt auf denselben Logical Date."""
    if isinstance(node, ast.Constant) and node.value is None:
        return True
    if isinstance(node, ast.Call) and call_name(node) == "timedelta":
        return timedelta_seconds(node) == 0
    return False


def _call_signals(call: ast.Call, path: str) -> list[Signal]:
    signals: list[Signal] = []
    name = call_name(call)
    is_sensor = name.endswith("ExternalTaskSensor")

    for kwarg in BOOL_SIGNALS:
        node = _kwarg_node(call, kwarg)
        if node and _is_true(node.value):
            signals.append(Signal(kind=kwarg, file=path, lineno=node.lineno, source="operator"))

    if is_sensor:
        for kwarg in sorted(OFFSET_KWARGS):
            node = _kwarg_node(call, kwarg)
            if node and not _is_zero_offset(node.value):
                signals.append(
                    Signal(
                        kind="external_task_sensor",
                        file=path,
                        lineno=node.lineno,
                        source="operator",
                    )
                )
        prior = _kwarg_node(call, "include_prior_dates")
        if prior and _is_true(prior.value):
            signals.append(
                Signal(
                    kind="include_prior_dates", file=path, lineno=prior.lineno, source="operator"
                )
            )

    signals.extend(_template_signals(call, path))
    return signals


def _argument_strings(call: ast.Call) -> list[ast.Constant]:
    """String-Literale in den Argumenten des Aufrufs, ohne in verschachtelte Aufrufe zu steigen.

    Sonst zaehlt ein Template doppelt, sobald ein Operator als Argument eines anderen
    Aufrufs steht (`chain(BashOperator(...))`).
    """
    found: list[ast.Constant] = []
    stack: list[ast.expr] = [*call.args, *(kw.value for kw in call.keywords)]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Call):
            continue
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                found.append(node)
            continue
        stack.extend(child for child in ast.iter_child_nodes(node) if isinstance(child, ast.expr))
    return found


def _template_signals(call: ast.Call, path: str) -> list[Signal]:
    signals: list[Signal] = []
    for node in _argument_strings(call):
        if isinstance(node.value, str):
            if PREV_SUCCESS.search(node.value):
                signals.append(
                    Signal(
                        kind="prev_run_success", file=path, lineno=node.lineno, source="template"
                    )
                )
            elif PREV_DATE.search(node.value):
                signals.append(
                    Signal(kind="prev_run_date", file=path, lineno=node.lineno, source="template")
                )
    return signals


def _context_param_signals(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda, path: str
) -> list[Signal]:
    args = node.args
    names = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    signals: list[Signal] = []
    for arg in names:
        if arg.arg in PREV_SUCCESS_PARAMS:
            kind = "prev_run_success"
        elif arg.arg in PREV_DATE_PARAMS:
            kind = "prev_run_date"
        else:
            continue
        signals.append(Signal(kind=kind, file=path, lineno=arg.lineno, source="context_param"))
    return signals


def _module_template_signals(tree: ast.Module, path: str) -> list[Signal]:
    """Template in einer Modul-Variablen statt im Operator-Argument (ADR-013)."""
    signals: list[Signal] = []
    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign) or not isinstance(stmt.value, ast.Constant):
            continue
        value = stmt.value.value
        if not isinstance(value, str):
            continue
        if PREV_SUCCESS.search(value):
            kind = "prev_run_success"
        elif PREV_DATE.search(value):
            kind = "prev_run_date"
        else:
            continue
        signals.append(
            Signal(kind=kind, file=path, lineno=stmt.value.lineno, source="module_template")
        )
    return signals


def _factory_signals(tree: ast.Module, path: str) -> list[Signal]:
    """ADR-009: Operator-Factories in Helper-Modulen, getrennt gezaehlt, keiner DAG-Zuordnung."""
    signals: list[Signal] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        returned = [
            stmt.value
            for stmt in ast.walk(node)
            if isinstance(stmt, ast.Return)
            and isinstance(stmt.value, ast.Call)
            and call_name(stmt.value).endswith(("Operator", "Sensor"))
        ]
        if not returned:
            continue

        args = node.args
        positional = args.posonlyargs + args.args
        with_default = positional[len(positional) - len(args.defaults) :] if args.defaults else []
        defaults = list(zip(with_default, args.defaults, strict=True))
        defaults += [
            (arg, default)
            for arg, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
            if default is not None
        ]
        for arg, default in defaults:
            if arg.arg in BOOL_SIGNALS and _is_true(default):
                signals.append(
                    Signal(kind=arg.arg, file=path, lineno=default.lineno, source="factory")
                )

        for call in returned:
            assert isinstance(call, ast.Call)
            for kwarg in BOOL_SIGNALS:
                keyword = _kwarg_node(call, kwarg)
                if keyword and _is_true(keyword.value):
                    signals.append(
                        Signal(kind=kwarg, file=path, lineno=keyword.lineno, source="factory")
                    )
    return list(dict.fromkeys(signals))


def analyze_source(source: str, path: str, dag_names: frozenset[str] = DAG_NAMES) -> FileAnalysis:
    """Ein Python-File analysieren. `path` ist der volle Repo-Pfad und wandert in jeden Beleg.

    `dag_names` sind die DAG-Konstruktoren des Repos: `DAG` plus die, die das Repo selbst
    definiert (ADR-015). Sie kommen aus einem Vorlauf ueber alle Files, weil der Konstruktor
    in einem anderen File steht als seine Aufrufe.
    """
    analysis = FileAnalysis()
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError) as err:  # ValueError: Nullbytes in fremden Files
        analysis.syntax_error = True
        analysis.errors.append(
            {
                "kind": "syntax_error",
                "file": path,
                "lineno": getattr(err, "lineno", None) or 0,
                "message": str(err)[:200],
            }
        )
        return analysis

    scopes = _collect_scopes(tree, path, analysis.errors, dag_names)
    if not scopes:
        analysis.factories = _factory_signals(tree, path)
        return analysis

    # Der DAG-Aufruf selbst traegt keine Task-Signale, sein default_args ist schon ausgewertet.
    dag_call_lines = {scope.finding.lineno for scope in scopes}

    single = scopes[0] if len(scopes) == 1 else None

    def attach(signals: list[Signal], owner: _Scope | None, anchor: int) -> None:
        """Signale dem DAG zuordnen. Ohne Zuordnung wird nicht geraten, sondern protokolliert."""
        if not signals:
            return
        inferred = False
        if owner is None:
            if single is None:
                analysis.errors.append(
                    {
                        "kind": "ambiguous_task",
                        "file": path,
                        "lineno": anchor,
                        "signals": sorted({s.kind for s in signals}),
                    }
                )
                return
            owner = single
            inferred = True
        for signal in signals:
            owner.finding.signals.append(
                Signal(
                    kind=signal.kind,
                    file=signal.file,
                    lineno=signal.lineno,
                    source=signal.source,
                    inferred=inferred,
                )
            )

    for signal in _module_template_signals(tree, path):
        attach([signal], _owner_of_line(signal.lineno, scopes), signal.lineno)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and any(
                _decorator_root(deco) == TASK_DECORATOR for deco in node.decorator_list
            ):
                task_owner = _owner_of_line(node.lineno, scopes) or single
                if task_owner:
                    task_owner.finding.task_count += 1
            attach(
                _context_param_signals(node, path),
                _owner_of_line(node.lineno, scopes),
                node.lineno,
            )
            continue
        if not isinstance(node, ast.Call) or node.lineno in dag_call_lines:
            continue
        if _is_task_call(node):
            owner = _owner(node, scopes) or single
            if owner:
                owner.finding.task_count += 1
        attach(_call_signals(node, path), _owner(node, scopes), node.lineno)

    for scope in scopes:
        scope.finding.signals = list(dict.fromkeys(scope.finding.signals))
    analysis.dags = [scope.finding for scope in scopes]
    return analysis


SKIP_DIRS = {".git", "node_modules", "venv", ".venv", "site-packages", "__pycache__", ".tox"}
MAX_FILE_BYTES = 1_000_000


def python_files(root: Path) -> list[Path]:
    files = []
    for path in sorted(root.rglob("*.py")):
        if SKIP_DIRS & set(path.parts) or not path.is_file():
            continue
        if path.stat().st_size > MAX_FILE_BYTES:
            continue
        files.append(path)
    return files


def repo_dag_names(files: list[Path]) -> frozenset[str]:
    """Vorlauf: welche DAG-Konstruktoren definiert dieses Repo selbst (ADR-015)?

    Ein Syntax-Fehler im Vorlauf wird uebergangen, nicht protokolliert: derselbe File laeuft
    gleich noch einmal durch `analyze_source`, dort wird er gemeldet.
    """
    scans: list[wrappers.ConstructorScan] = []
    for path in files:
        try:
            tree = ast.parse(path.read_bytes().decode("utf-8", "replace"))
        except (SyntaxError, ValueError):
            continue
        scans.append(wrappers.scan_file(tree))
    return DAG_NAMES | wrappers.resolve(scans)


def analyze_repo(root: Path, repo: str) -> RepoAnalysis:
    files = python_files(root)
    dag_names = repo_dag_names(files)
    result = RepoAnalysis(repo=repo, dag_names=dag_names)
    for path in files:
        rel = path.relative_to(root).as_posix()
        source = path.read_bytes().decode("utf-8", "replace")
        analysis = analyze_source(source, rel, dag_names)
        result.files_parsed += 1
        result.syntax_errors += int(analysis.syntax_error)
        result.dags.extend(analysis.dags)
        result.factories.extend(analysis.factories)
        for err in analysis.errors:
            result.errors.append({"repo": repo, **err})
    return result
