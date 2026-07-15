"""Airflow-Parser: vom DAG-File zur Pipeline-Struktur (Spec 007).

Der Parser teilt mit dem Scanner die Signal-Definition (wiki/signals.md), nicht die
Extraktion: er braucht die Task-Zuordnung jedes Signals, das Ziel jedes Sensors und
den Wert jedes `execution_delta`. Leitregel: der Parser darf weniger wissen, als im
File steht, aber nie mehr. Was nicht statisch aufloesbar ist, wird als `Warning_`
mit Datei und Zeile gemeldet, nicht geraten — eine weggelassene Kante laesst Lambda
eine gueltige Untergrenze (wiki/math.md, Abschnitt 8), eine erfundene nicht.

Uebersetzung Signal -> Kante: cc-sessions/007, Tabelle. F erzeugt bewusst keine
Kante (ADR-020): das Template rendert einen Zeitstempel und wartet nicht.

Fremde DAG-Files sind Systemgrenze: Syntax-Fehler werden als Warnung gemeldet.
"""

from __future__ import annotations

import ast
import re
import warnings
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from eigenlag.model import CrossEdge, Pipeline
from eigenlag.schedule import call_name, period_seconds, timedelta_seconds

DAG_NAMES = frozenset({"DAG"})
TASK_CALL_SUFFIXES = ("Operator", "Sensor")
SCHEDULE_KWARGS = ("schedule", "schedule_interval", "timetable")
SKIP_DIRS = {".git", "node_modules", "venv", ".venv", "site-packages", "__pycache__", ".tox"}

# Signal F: dieselben Muster wie im Scanner (wiki/signals.md, ADR-013). Bewusst
# dupliziert statt importiert — die Abhaengigkeit laeuft Produkt <- Scanner, nie
# umgekehrt. Der Konsistenz-Test in scanner/ haelt beide Definitionen deckungsgleich.
PREV_SUCCESS = re.compile(
    r"\{\{[^{}]*\bprev_(start_date_success|data_interval_start_success|"
    r"data_interval_end_success)\b"
)
PREV_DATE = re.compile(r"\{\{[^{}]*\bprev_(ds|ds_nodash|execution_date)\b")
PREV_SUCCESS_PARAMS = {
    "prev_start_date_success",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
}
PREV_DATE_PARAMS = {"prev_ds", "prev_ds_nodash", "prev_execution_date"}


@dataclass(frozen=True)
class Warning_:
    """Erkannt, aber nicht modelliert — mit Fundstelle, damit David es nachschlagen kann."""

    kind: str
    file: str
    lineno: int
    detail: str = ""


@dataclass(frozen=True)
class ParsedCrossEdge:
    """CrossEdge plus Herkunft: welches Signal, in welcher Datei, auf welcher Zeile."""

    src: str
    dst: str
    periods: int
    signal: str
    file: str
    lineno: int


@dataclass(frozen=True)
class ParsedDag:
    dag_id: str | None  # None, wenn nicht statisch aufloesbar — nicht raten
    file: str
    lineno: int
    schedule_expr: str | None
    period_s: float | None
    tasks: tuple[str, ...]
    intra: tuple[tuple[str, str], ...]
    cross: tuple[ParsedCrossEdge, ...]
    warnings: tuple[Warning_, ...]


@dataclass(frozen=True)
class ParseResult:
    dags: tuple[ParsedDag, ...]
    warnings: tuple[Warning_, ...]  # File-Ebene: syntax_error, dag_not_airflow, ambiguous_task


# --- interne Bau-Strukturen ----------------------------------------------------------


@dataclass
class _Draft:
    dag_id: str | None
    file: str
    lineno: int
    schedule_expr: str | None
    period_s: float | None
    span: tuple[int, int] | None
    var: str | None
    tasks: list[str] = field(default_factory=list)
    intra: list[tuple[str, str]] = field(default_factory=list)
    warnings: list[Warning_] = field(default_factory=list)
    # Effektive Flags je Task: None = nicht am Operator gesetzt, default_args gilt.
    dop_flags: dict[str, bool] = field(default_factory=dict)
    wfd_flags: dict[str, bool] = field(default_factory=dict)
    dop_default: bool = False
    wfd_default: bool = False
    serialized: bool = False  # max_active_runs=1
    sensors: list[_SensorRef] = field(default_factory=list)
    signal_linenos: dict[str, int] = field(default_factory=dict)

    def add_task(self, name: str, lineno: int) -> None:
        if name not in self.tasks:
            self.tasks.append(name)
        self.signal_linenos.setdefault(f"task:{name}", lineno)

    def warn(self, kind: str, lineno: int, detail: str = "") -> None:
        self.warnings.append(Warning_(kind=kind, file=self.file, lineno=lineno, detail=detail))


@dataclass(frozen=True)
class _SensorRef:
    task: str
    external_dag_id: str | None
    external_task_id: str | None
    delta_s: float | None  # None = gesetzt, aber nicht statisch aufloesbar
    delta_set: bool
    date_fn: bool
    lineno: int


@dataclass
class _TaskRef:
    name: str
    draft: _Draft
    dynamic: bool = False


@dataclass
class _GroupRef:
    prefix: str
    draft: _Draft
    tasks: list[str] = field(default_factory=list)


@dataclass
class _ListRef:
    items: list[_Ref]


@dataclass
class _TaskFactory:
    name: str  # @task-dekorierte Funktion; die Task-Instanz entsteht beim Aufruf


@dataclass
class _DagRef:
    draft: _Draft


_Ref = _TaskRef | _GroupRef | _ListRef | _TaskFactory | _DagRef | None


# --- Import-Beleg: DAG muss aus airflow kommen ---------------------------------------


def _airflow_names(tree: ast.Module) -> tuple[dict[str, str], set[str]]:
    """Bindungen des Files: Name -> Modul-Herkunft, plus lokal definierte Namen."""
    origins: dict[str, str] = {}
    local: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                origins[alias.asname or alias.name] = node.module
        elif isinstance(node, ast.Import):
            for alias in node.names:
                origins[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            local.add(node.name)
    return origins, local


def _from_airflow(name: str, origins: dict[str, str], local: set[str]) -> bool:
    """Positiver Beleg gefordert: ohne airflow-Import zaehlt der Name nicht.

    Verhindert den 330-Zeilen-Fehler aus Session 006: ein generierter API-Client
    mit einer Modellklasse namens `DAG` ist kein Airflow-Repo.
    """
    if name in local:
        return False
    origin = origins.get(name)
    return origin is not None and (origin == "airflow" or origin.startswith("airflow."))


# --- kleine AST-Helfer ---------------------------------------------------------------


def _kwarg(call: ast.Call, name: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _is_true(node: ast.expr | None) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


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


def _module_dicts(tree: ast.Module) -> dict[str, ast.Dict]:
    found: dict[str, ast.Dict] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Dict):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    found[target.id] = stmt.value
    return found


def _decorator_root(node: ast.expr) -> str:
    current = node.func if isinstance(node, ast.Call) else node
    while isinstance(current, ast.Attribute):
        current = current.value
    return current.id if isinstance(current, ast.Name) else ""


def _template_spans(tree: ast.Module, dag_names: frozenset[str]) -> list[tuple[int, int]]:
    """Funktionen, deren eigener Rumpf ein DAG(...) zurueckgibt, sind Konstruktoren.

    Das DAG(...) darin ist Schablone, kein DAG des Files (ADR-015). Ein `return` in
    einer verschachtelten Funktion zaehlt nicht fuer die aeussere.
    """
    spans: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for stmt in node.body:
            for inner in ast.walk(stmt):
                if isinstance(inner, ast.FunctionDef | ast.AsyncFunctionDef):
                    break
                if (
                    isinstance(inner, ast.Return)
                    and isinstance(inner.value, ast.Call)
                    and call_name(inner.value) in dag_names
                    and node.end_lineno is not None
                ):
                    spans.append((node.lineno, node.end_lineno))
                    break
    return spans


def _schedule_of(call: ast.Call) -> tuple[str | None, float | None]:
    for name in SCHEDULE_KWARGS:
        node = _kwarg(call, name)
        if node is None:
            continue
        raw = ast.unparse(node)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return raw, period_seconds(node.value)
        if isinstance(node, ast.Call) and call_name(node) == "timedelta":
            seconds = timedelta_seconds(node)
            return raw, seconds if seconds is not None and seconds > 0 else None
        return raw, None
    return None, None


def _dag_id_of(call: ast.Call, strings: dict[str, str], fallback: str | None) -> str | None:
    node = _kwarg(call, "dag_id")
    if node is None and call.args:
        node = call.args[0]
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return strings.get(node.id, fallback)
    return fallback


# --- der eigentliche Parser ----------------------------------------------------------


class _FileParser:
    def __init__(self, tree: ast.Module, path: str, dag_names: frozenset[str]) -> None:
        self.tree = tree
        self.path = path
        self.dag_names = dag_names
        self.origins, self.local_defs = _airflow_names(tree)
        self.strings = _module_strings(tree)
        self.dicts = _module_dicts(tree)
        self.templates = _template_spans(tree, dag_names)
        self.drafts: list[_Draft] = []
        self.file_warnings: list[Warning_] = []
        self.env: dict[str, _Ref] = {}
        self._draft_at: dict[int, _Draft] = {}

    # -- Phase 1: DAG-Scopes einsammeln ------------------------------------------------

    def _is_template(self, lineno: int) -> bool:
        return any(start <= lineno <= end for start, end in self.templates)

    def _dag_call_ok(self, call: ast.Call) -> bool:
        name = call_name(call)
        if name not in self.dag_names or self._is_template(call.lineno):
            return False
        if name != "DAG":
            return True  # Repo-Konstruktor (ADR-015), vom Vorlauf aufgeloest
        if isinstance(call.func, ast.Attribute):  # airflow.DAG(...) o.ae.
            root = call.func.value
            return isinstance(root, ast.Name) and _from_airflow(
                root.id, self.origins, self.local_defs
            )
        if _from_airflow("DAG", self.origins, self.local_defs):
            return True
        self._warn_file("dag_not_airflow", call.lineno, "DAG ist nicht aus airflow importiert")
        return False

    def _warn_file(self, kind: str, lineno: int, detail: str = "") -> None:
        self.file_warnings.append(Warning_(kind=kind, file=self.path, lineno=lineno, detail=detail))

    def _new_draft(
        self,
        call: ast.Call,
        span: tuple[int, int] | None,
        var: str | None,
        fallback_id: str | None = None,
    ) -> _Draft:
        schedule_expr, period_s = _schedule_of(call)
        draft = _Draft(
            dag_id=_dag_id_of(call, self.strings, fallback_id),
            file=self.path,
            lineno=call.lineno,
            schedule_expr=schedule_expr,
            period_s=period_s,
            span=span,
            var=var,
        )
        node = _kwarg(call, "max_active_runs")
        if (
            node is not None
            and isinstance(node, ast.Constant)
            and not isinstance(node.value, bool)
            and node.value == 1
        ):
            draft.serialized = True
            draft.signal_linenos["max_active_runs"] = node.lineno
        self._default_args(call, draft)
        self.drafts.append(draft)
        self._draft_at[call.lineno] = draft
        return draft

    def _default_args(self, call: ast.Call, draft: _Draft) -> None:
        node = _kwarg(call, "default_args")
        if node is None:
            return
        literal = node if isinstance(node, ast.Dict) else None
        if literal is None and isinstance(node, ast.Name):
            literal = self.dicts.get(node.id)
        if literal is None:
            draft.warn("unresolved_default_args", node.lineno, ast.unparse(node))
            return
        for key, value in zip(literal.keys, literal.values, strict=True):
            if not (isinstance(key, ast.Constant) and isinstance(value, ast.Constant)):
                continue
            if key.value == "depends_on_past" and value.value is True:
                draft.dop_default = True
                draft.signal_linenos["depends_on_past"] = key.lineno
            if key.value == "wait_for_downstream" and value.value is True:
                draft.wfd_default = True
                draft.signal_linenos["wait_for_downstream"] = key.lineno

    def collect_scopes(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.With | ast.AsyncWith):
                for item in node.items:
                    call = item.context_expr
                    if not isinstance(call, ast.Call) or not self._dag_call_ok(call):
                        continue
                    var = (
                        item.optional_vars.id if isinstance(item.optional_vars, ast.Name) else None
                    )
                    end = node.end_lineno if node.end_lineno is not None else node.lineno
                    self._new_draft(call, (node.lineno, end), var)
            elif isinstance(node, ast.Assign):
                assigned = node.value
                if not isinstance(assigned, ast.Call) or not self._dag_call_ok(assigned):
                    continue
                target = node.targets[0]
                var = target.id if isinstance(target, ast.Name) else None
                self._new_draft(assigned, None, var)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                for deco in node.decorator_list:
                    name = (
                        call_name(deco)
                        if isinstance(deco, ast.Call)
                        else (deco.id if isinstance(deco, ast.Name) else "")
                    )
                    if name != "dag" or not _from_airflow("dag", self.origins, self.local_defs):
                        continue
                    call = (
                        deco
                        if isinstance(deco, ast.Call)
                        else ast.Call(func=ast.Name(id="dag"), args=[], keywords=[])
                    )
                    call.lineno = deco.lineno
                    end = node.end_lineno if node.end_lineno is not None else node.lineno
                    self._new_draft(call, (node.lineno, end), None, fallback_id=node.name)

    # -- Zuordnung ---------------------------------------------------------------------

    def _owner_of_line(self, lineno: int) -> _Draft | None:
        lexical = [d for d in self.drafts if d.span and d.span[0] <= lineno <= d.span[1]]
        if not lexical:
            return None
        return max(lexical, key=lambda d: d.span[0])  # type: ignore[index]  # span geprueft

    def _owner(self, call: ast.Call, scope: _Draft | None) -> tuple[_Draft | None, bool]:
        """Besitzer eines Task-Aufrufs, plus ob die Zuordnung inferiert wurde."""
        if scope is not None:
            return scope, False
        bound = _kwarg(call, "dag")
        if isinstance(bound, ast.Name):
            ref = self.env.get(bound.id)
            if isinstance(ref, _DagRef):
                return ref.draft, False
            for draft in self.drafts:
                if draft.var == bound.id:
                    return draft, False
        if len(self.drafts) == 1:
            return self.drafts[0], True
        self._warn_file(
            "ambiguous_task",
            call.lineno,
            "keine DAG-Zuordnung moeglich, mehrere oder keine DAGs im File",
        )
        return None, False

    # -- Phase 2: strukturierter Lauf ueber die Statements ------------------------------

    def walk_module(self) -> None:
        self._walk_body(self.tree.body, scope=None, prefix="")
        self._module_template_signals()

    def _walk_body(self, body: Sequence[ast.stmt], scope: _Draft | None, prefix: str) -> None:
        for stmt in body:
            self._walk_stmt(stmt, scope, prefix)

    def _walk_stmt(self, stmt: ast.stmt, scope: _Draft | None, prefix: str) -> None:
        if isinstance(stmt, ast.With | ast.AsyncWith):
            inner_scope, inner_prefix = scope, prefix
            for item in stmt.items:
                call = item.context_expr
                if not isinstance(call, ast.Call):
                    continue
                draft = self._draft_at.get(call.lineno)
                if draft is not None and call_name(call) in self.dag_names:
                    inner_scope = draft
                    if isinstance(item.optional_vars, ast.Name):
                        self.env[item.optional_vars.id] = _DagRef(draft)
                elif call_name(call) == "TaskGroup":
                    group_id = self._group_id(call)
                    owner, _ = self._owner(call, inner_scope)
                    if group_id is None or owner is None:
                        continue
                    group = _GroupRef(prefix=f"{inner_prefix}{group_id}.", draft=owner)
                    if isinstance(item.optional_vars, ast.Name):
                        self.env[item.optional_vars.id] = group
                    self._walk_group(stmt, owner, group)
                    return
                else:
                    self._eval(call, inner_scope, inner_prefix)
            self._walk_body(stmt.body, inner_scope, inner_prefix)
        elif isinstance(stmt, ast.Assign | ast.AnnAssign):
            value = stmt.value
            if value is None:
                return
            draft = self._draft_at.get(value.lineno) if isinstance(value, ast.Call) else None
            ref = _DagRef(draft) if draft is not None else self._eval(value, scope, prefix)
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            for target in targets:
                if isinstance(target, ast.Name) and ref is not None:
                    self.env[target.id] = ref
        elif isinstance(stmt, ast.Expr):
            if isinstance(stmt.value, ast.Call) and self._draft_at.get(stmt.value.lineno):
                return  # DAG-Aufruf ohne Zuweisung
            self._eval(stmt.value, scope, prefix)
        elif isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            self._function_def(stmt, scope, prefix)
        elif isinstance(stmt, ast.For | ast.AsyncFor | ast.While | ast.If):
            self._walk_body(stmt.body, scope, prefix)
            self._walk_body(stmt.orelse, scope, prefix)
        elif isinstance(stmt, ast.Try):
            self._walk_body(stmt.body, scope, prefix)
            for handler in stmt.handlers:
                self._walk_body(handler.body, scope, prefix)
            self._walk_body(stmt.orelse, scope, prefix)
            self._walk_body(stmt.finalbody, scope, prefix)
        elif isinstance(stmt, ast.ClassDef):
            self._walk_body(stmt.body, scope, prefix)
        elif isinstance(stmt, ast.Return) and stmt.value is not None:
            self._eval(stmt.value, scope, prefix)

    def _walk_group(self, stmt: ast.With | ast.AsyncWith, owner: _Draft, group: _GroupRef) -> None:
        before = set(owner.tasks)
        self._walk_body(stmt.body, owner, group.prefix)
        group.tasks = [t for t in owner.tasks if t not in before]

    def _group_id(self, call: ast.Call) -> str | None:
        node = _kwarg(call, "group_id")
        if node is None and call.args:
            node = call.args[0]
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _function_def(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, scope: _Draft | None, prefix: str
    ) -> None:
        self._context_param_signals(node, scope)
        draft = None
        for deco in node.decorator_list:
            root = _decorator_root(deco)
            if root == "dag":
                draft = self._draft_at.get(deco.lineno)
            elif root == "task" and _from_airflow("task", self.origins, self.local_defs):
                self.env[node.name] = _TaskFactory(name=node.name)
            elif root == "task_group":
                owner = scope or (self.drafts[0] if len(self.drafts) == 1 else None)
                if owner is not None:
                    group = _GroupRef(prefix=f"{prefix}{node.name}.", draft=owner)
                    before = set(owner.tasks)
                    self._walk_body(node.body, owner, group.prefix)
                    group.tasks = [t for t in owner.tasks if t not in before]
                    self.env[node.name] = group
                return
        if draft is not None:
            self._walk_body(node.body, draft, prefix)
        else:
            self._walk_body(node.body, scope, prefix)

    # -- Ausdruecke: Tasks, Kanten, Signale ---------------------------------------------

    def _eval(self, node: ast.expr, scope: _Draft | None, prefix: str) -> _Ref:
        if isinstance(node, ast.Name):
            return self.env.get(node.id)
        if isinstance(node, ast.List | ast.Tuple):
            items = [self._eval(e, scope, prefix) for e in node.elts]
            return _ListRef(items=items)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift | ast.LShift):
            left = self._eval(node.left, scope, prefix)
            right = self._eval(node.right, scope, prefix)
            if isinstance(node.op, ast.RShift):
                self._connect(left, right, node.lineno, scope)
            else:
                self._connect(right, left, node.lineno, scope)
            return right
        if isinstance(node, ast.Call):
            return self._eval_call(node, scope, prefix)
        if isinstance(node, ast.Lambda):
            self._context_param_signals(node, scope)
            return None
        return None

    def _eval_call(self, call: ast.Call, scope: _Draft | None, prefix: str) -> _Ref:
        name = call_name(call)
        func = call.func

        if name == "chain":
            refs = [self._eval(a, scope, prefix) for a in call.args]
            for left, right in zip(refs, refs[1:], strict=False):
                self._chain_pair(left, right, call.lineno, scope)
            return None

        if isinstance(func, ast.Attribute) and func.attr in ("set_downstream", "set_upstream"):
            obj = self._eval(func.value, scope, prefix)
            for arg in call.args:
                other = self._eval(arg, scope, prefix)
                if func.attr == "set_downstream":
                    self._connect(obj, other, call.lineno, scope)
                else:
                    self._connect(other, obj, call.lineno, scope)
            return obj

        if isinstance(func, ast.Attribute) and func.attr in ("expand", "expand_kwargs"):
            return self._mapped_task(call, scope, prefix)

        if isinstance(func, ast.Name):
            bound = self.env.get(func.id)
            if isinstance(bound, _TaskFactory):
                owner, inferred = self._owner(call, scope)
                if owner is None:
                    return None
                task = f"{prefix}{bound.name}"
                owner.add_task(task, call.lineno)
                if inferred:
                    owner.warn("task_dag_inferred", call.lineno, task)
                return _TaskRef(name=task, draft=owner)
            if isinstance(bound, _GroupRef | _DagRef):
                return bound

        if name.endswith(TASK_CALL_SUFFIXES):
            for arg in call.args:
                self._eval(arg, scope, prefix)
            for kw in call.keywords:
                self._eval(kw.value, scope, prefix)
            return self._operator_task(call, scope, prefix)

        for arg in call.args:
            self._eval(arg, scope, prefix)
        for kw in call.keywords:
            self._eval(kw.value, scope, prefix)
        return None

    def _task_id_of(self, call: ast.Call, owner: _Draft, prefix: str) -> tuple[str | None, bool]:
        """Task-Name, plus ob er dynamisch (Platzhalter) ist."""
        node = _kwarg(call, "task_id")
        if node is None and call.args and not isinstance(call.args[0], ast.Starred):
            node = call.args[0]
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return f"{prefix}{node.value}", False
        if isinstance(node, ast.Name) and node.id in self.strings:
            return f"{prefix}{self.strings[node.id]}", False
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for value in node.values:
                if isinstance(value, ast.Constant):
                    parts.append(str(value.value))
                elif isinstance(value, ast.FormattedValue):
                    parts.append("{" + ast.unparse(value.value) + "}")
            placeholder = f"{prefix}{''.join(parts)}?"
            owner.warn("dynamic_task_id", node.lineno, placeholder)
            return placeholder, True
        if node is not None:
            placeholder = f"{prefix}{ast.unparse(node)}?"
            owner.warn("dynamic_task_id", node.lineno, placeholder)
            return placeholder, True
        return None, False

    def _operator_task(self, call: ast.Call, scope: _Draft | None, prefix: str) -> _Ref:
        owner, inferred = self._owner(call, scope)
        if owner is None:
            return None
        task, dynamic = self._task_id_of(call, owner, prefix)
        if task is None:
            return None
        owner.add_task(task, call.lineno)
        if inferred:
            owner.warn("task_dag_inferred", call.lineno, task)
        self._operator_flags(call, owner, task)
        self._template_signals(call, owner)
        if call_name(call).endswith("ExternalTaskSensor"):
            self._sensor(call, owner, task)
        return _TaskRef(name=task, draft=owner, dynamic=dynamic)

    def _mapped_task(self, call: ast.Call, scope: _Draft | None, prefix: str) -> _Ref:
        """Dynamic Task Mapping: eine Task, Warnung `task_mapping` (konservativ korrekt,
        solange die Dauer die der ganzen Mapping-Stufe ist)."""
        func = call.func
        assert isinstance(func, ast.Attribute)
        base = func.value
        inner = None
        if (
            isinstance(base, ast.Call)
            and isinstance(base.func, ast.Attribute)
            and base.func.attr == "partial"
        ):
            inner = base
        if inner is not None:
            ref = self._operator_partial(inner, call, scope, prefix)
            return ref
        if isinstance(base, ast.Name):
            bound = self.env.get(base.id)
            if isinstance(bound, _TaskFactory):
                owner, inferred = self._owner(call, scope)
                if owner is None:
                    return None
                task = f"{prefix}{bound.name}"
                owner.add_task(task, call.lineno)
                owner.warn("task_mapping", call.lineno, task)
                if inferred:
                    owner.warn("task_dag_inferred", call.lineno, task)
                return _TaskRef(name=task, draft=owner)
        return None

    def _operator_partial(
        self, partial: ast.Call, expand: ast.Call, scope: _Draft | None, prefix: str
    ) -> _Ref:
        owner, inferred = self._owner(partial, scope)
        if owner is None:
            return None
        task, dynamic = self._task_id_of(partial, owner, prefix)
        if task is None:
            return None
        owner.add_task(task, expand.lineno)
        owner.warn("task_mapping", expand.lineno, task)
        if inferred:
            owner.warn("task_dag_inferred", expand.lineno, task)
        self._operator_flags(partial, owner, task)
        self._template_signals(partial, owner)
        self._template_signals(expand, owner)
        return _TaskRef(name=task, draft=owner, dynamic=dynamic)

    def _operator_flags(self, call: ast.Call, owner: _Draft, task: str) -> None:
        for kwarg, flags in (
            ("depends_on_past", owner.dop_flags),
            ("wait_for_downstream", owner.wfd_flags),
        ):
            node = _kwarg(call, kwarg)
            if isinstance(node, ast.Constant) and isinstance(node.value, bool):
                flags[task] = node.value
                if node.value:
                    owner.signal_linenos.setdefault(kwarg, node.lineno)
                    owner.signal_linenos[f"{kwarg}:{task}"] = node.lineno

    def _sensor(self, call: ast.Call, owner: _Draft, task: str) -> None:
        ext_dag = self._static_str(_kwarg(call, "external_dag_id"))
        ext_task = self._static_str(_kwarg(call, "external_task_id"))
        delta_node = _kwarg(call, "execution_delta")
        date_fn = _kwarg(call, "execution_date_fn") is not None
        delta_set = delta_node is not None and not (
            isinstance(delta_node, ast.Constant) and delta_node.value is None
        )
        delta_s: float | None = None
        if delta_set and delta_node is not None:
            if isinstance(delta_node, ast.Call) and call_name(delta_node) == "timedelta":
                delta_s = timedelta_seconds(delta_node)
            else:
                delta_s = None
        prior = _kwarg(call, "include_prior_dates")
        if _is_true(prior):
            owner.warn(
                "include_prior_dates",
                call.lineno,
                "any earlier run suffices; an edge would falsely raise λ",
            )
        if delta_set or date_fn:
            owner.sensors.append(
                _SensorRef(
                    task=task,
                    external_dag_id=ext_dag,
                    external_task_id=ext_task,
                    delta_s=delta_s,
                    delta_set=delta_set,
                    date_fn=date_fn,
                    lineno=call.lineno,
                )
            )

    def _static_str(self, node: ast.expr | None) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return self.strings.get(node.id)
        return None

    # -- Kanten ------------------------------------------------------------------------

    def _endpoints(self, ref: _Ref, side: str) -> tuple[list[str], _Draft | None, bool]:
        """Konkrete Task-Namen eines Kanten-Endes: (Namen, Draft, dynamisch-dabei)."""
        if isinstance(ref, _TaskRef):
            if ref.dynamic:
                return [], ref.draft, True
            return [ref.name], ref.draft, False
        if isinstance(ref, _GroupRef):
            inside = set(ref.tasks)
            linked = {(s, d) for (s, d) in ref.draft.intra if s in inside and d in inside}
            if side == "sink":
                names = [t for t in ref.tasks if not any(s == t for s, _ in linked)]
            else:
                names = [t for t in ref.tasks if not any(d == t for _, d in linked)]
            return names, ref.draft, False
        if isinstance(ref, _ListRef):
            collected: list[str] = []
            draft: _Draft | None = None
            dynamic = False
            for item in ref.items:
                sub, sub_draft, sub_dyn = self._endpoints(item, side)
                collected.extend(sub)
                draft = draft or sub_draft
                dynamic = dynamic or sub_dyn
            return collected, draft, dynamic
        return [], None, ref is not None

    def _connect(self, left: _Ref, right: _Ref, lineno: int, scope: _Draft | None) -> None:
        if left is None or right is None:
            if left is not None or right is not None:
                draft = self._ref_draft(left) or self._ref_draft(right) or scope
                if draft is not None:
                    draft.warn("edge_dropped", lineno, "edge endpoint not statically resolvable")
            return
        sinks, left_draft, left_dyn = self._endpoints(left, "sink")
        sources, right_draft, right_dyn = self._endpoints(right, "source")
        draft = left_draft or right_draft or scope
        if draft is None:
            return
        if left_dyn or right_dyn:
            draft.warn("edge_dropped", lineno, "edge lapses at a dynamic task")
        if left_draft is not None and right_draft is not None and left_draft is not right_draft:
            draft.warn("edge_dropped", lineno, "edge connects two DAGs")
            return
        for s in sinks:
            for q in sources:
                if (s, q) not in draft.intra:
                    draft.intra.append((s, q))

    def _ref_draft(self, ref: _Ref) -> _Draft | None:
        if isinstance(ref, _TaskRef | _GroupRef):
            return ref.draft
        if isinstance(ref, _ListRef):
            for item in ref.items:
                found = self._ref_draft(item)
                if found is not None:
                    return found
        return None

    def _chain_pair(self, left: _Ref, right: _Ref, lineno: int, scope: _Draft | None) -> None:
        """Airflow-`chain`: Liste gegen Liste paart elementweise, sonst alle gegen alle."""
        if isinstance(left, _ListRef) and isinstance(right, _ListRef):
            if len(left.items) != len(right.items):
                draft = self._ref_draft(left) or self._ref_draft(right) or scope
                if draft is not None:
                    draft.warn("edge_dropped", lineno, "chain: lists of unequal length")
                return
            for a, b in zip(left.items, right.items, strict=True):
                self._connect(a, b, lineno, scope)
            return
        self._connect(left, right, lineno, scope)

    # -- Signal F ------------------------------------------------------------------------

    def _argument_strings(self, call: ast.Call) -> list[ast.Constant]:
        found: list[ast.Constant] = []
        stack: list[ast.expr] = [*call.args, *(kw.value for kw in call.keywords)]
        while stack:
            node = stack.pop()
            if isinstance(node, ast.Call):
                continue  # verschachtelte Aufrufe zaehlen ihre Templates selbst
            if isinstance(node, ast.Constant):
                if isinstance(node.value, str):
                    found.append(node)
                continue
            stack.extend(
                child for child in ast.iter_child_nodes(node) if isinstance(child, ast.expr)
            )
        return found

    def _template_signals(self, call: ast.Call, owner: _Draft) -> None:
        for node in self._argument_strings(call):
            if not isinstance(node.value, str):
                continue
            self._prev_warning(node.value, owner, node.lineno)

    def _prev_warning(self, text: str, owner: _Draft, lineno: int) -> None:
        if PREV_SUCCESS.search(text):
            # ADR-020: Datenabhaengigkeit ohne Wartesemantik — Befund, keine Lambda-Kante.
            owner.warn("prev_run_success", lineno, "data dependency without wait semantics")
        elif PREV_DATE.search(text):
            owner.warn("prev_run_date", lineno, "weak signal, pure date arithmetic")

    def _context_param_signals(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda, scope: _Draft | None
    ) -> None:
        args = node.args
        names = [*args.posonlyargs, *args.args, *args.kwonlyargs]
        owner = scope or self._owner_of_line(node.lineno)
        if owner is None and len(self.drafts) == 1:
            owner = self.drafts[0]
        if owner is None:
            return
        for arg in names:
            if arg.arg in PREV_SUCCESS_PARAMS:
                owner.warn("prev_run_success", arg.lineno, f"context parameter {arg.arg}")
            elif arg.arg in PREV_DATE_PARAMS:
                owner.warn("prev_run_date", arg.lineno, f"context parameter {arg.arg}")

    def _module_template_signals(self) -> None:
        for stmt in self.tree.body:
            if not isinstance(stmt, ast.Assign) or not isinstance(stmt.value, ast.Constant):
                continue
            value = stmt.value.value
            if not isinstance(value, str):
                continue
            if not (PREV_SUCCESS.search(value) or PREV_DATE.search(value)):
                continue
            owner = self._owner_of_line(stmt.lineno)
            if owner is None and len(self.drafts) == 1:
                owner = self.drafts[0]
            if owner is not None:
                self._prev_warning(value, owner, stmt.value.lineno)


# --- Verknuepfung: aus Drafts werden ParsedDags --------------------------------------


def _direct_downstream(draft: _Draft, task: str) -> list[str]:
    return [d for (s, d) in draft.intra if s == task]


def _cross_edges(draft: _Draft, drafts: Sequence[_Draft]) -> list[ParsedCrossEdge]:
    edges: list[ParsedCrossEdge] = []

    def line(key: str, fallback: str) -> int:
        return draft.signal_linenos.get(key, draft.signal_linenos.get(fallback, draft.lineno))

    modeled_a = False
    modeled_b = False
    for task in draft.tasks:
        wfd = draft.wfd_flags.get(task, draft.wfd_default)
        dop = draft.dop_flags.get(task, draft.dop_default)
        if wfd:
            # B impliziert A: Selbstkante plus die direkten Downstreams des Vorlaufs.
            modeled_b = True
            lineno = line(f"wait_for_downstream:{task}", "wait_for_downstream")
            edges.append(ParsedCrossEdge(task, task, 1, "wait_for_downstream", draft.file, lineno))
            for down in _direct_downstream(draft, task):
                edges.append(
                    ParsedCrossEdge(down, task, 1, "wait_for_downstream", draft.file, lineno)
                )
        if dop:
            # Auch wenn B die Selbstkante impliziert: erkannt sind beide Signale,
            # und die Selbstkante traegt A, sonst weicht der Parser vom Scanner ab.
            modeled_a = True
            lineno = line(f"depends_on_past:{task}", "depends_on_past")
            edges.append(ParsedCrossEdge(task, task, 1, "depends_on_past", draft.file, lineno))

    # Erkannt, aber ohne Task keine Kante: als Warnung melden, nicht verschlucken.
    if draft.dop_default and not modeled_a and not modeled_b:
        draft.warn(
            "depends_on_past",
            line("depends_on_past", "depends_on_past"),
            "detected in default_args, but no task carries an edge",
        )
    if draft.wfd_default and not modeled_b:
        draft.warn(
            "wait_for_downstream",
            line("wait_for_downstream", "wait_for_downstream"),
            "detected in default_args, but no task carries an edge",
        )

    if draft.serialized:
        lineno = line("max_active_runs", "max_active_runs")
        if draft.tasks:
            has_out = {s for (s, _) in draft.intra}
            has_in = {d for (_, d) in draft.intra}
            sinks = [t for t in draft.tasks if t not in has_out]
            sources = [t for t in draft.tasks if t not in has_in]
            for s in sinks:
                for q in sources:
                    edges.append(ParsedCrossEdge(s, q, 1, "max_active_runs", draft.file, lineno))
        else:
            draft.warn("max_active_runs", lineno, "detected, but the DAG has no tasks")

    edges.extend(_sensor_edges(draft, drafts))
    return edges


def _sensor_edges(draft: _Draft, drafts: Sequence[_Draft]) -> list[ParsedCrossEdge]:
    edges: list[ParsedCrossEdge] = []
    for sensor in draft.sensors:
        if sensor.date_fn:
            draft.warn(
                "sensor_dynamic_offset",
                sensor.lineno,
                "execution_date_fn: return value not statically determinable",
            )
        if not sensor.delta_set:
            continue

        def unmodeled(reason: str, sensor: _SensorRef = sensor) -> None:
            draft.warn("sensor_not_modeled", sensor.lineno, reason)

        if sensor.delta_s is None:
            unmodeled("execution_delta not statically resolvable")
            continue
        if sensor.delta_s == 0:
            continue  # Versatz null zeigt auf denselben Logical Date: Intra-Run, kein Signal
        if sensor.external_dag_id is None:
            unmodeled("external_dag_id not statically resolvable")
            continue
        if sensor.external_dag_id == draft.dag_id:
            # ADR-021: Selbst-Referenz — beide Enden im eigenen DAG, T-Gleichheit und
            # Merge-Frage entfallen per Konstruktion. Lauf k wartet auf Lauf k - n.
            if draft.period_s is None:
                unmodeled("the DAG's period cannot be determined")
                continue
            ratio = sensor.delta_s / draft.period_s
            if ratio < 1 or abs(ratio - round(ratio)) > 1e-9:
                unmodeled(f"execution_delta/T = {ratio:g} is not an integer >= 1")
                continue
            if sensor.external_task_id is None:
                unmodeled("external_task_id missing or not statically resolvable")
                continue
            if sensor.external_task_id not in draft.tasks:
                unmodeled(f"target task {sensor.external_task_id!r} not found in the DAG")
                continue
            edges.append(
                ParsedCrossEdge(
                    src=f"{draft.dag_id}.{sensor.external_task_id}",
                    dst=sensor.task,
                    periods=int(round(ratio)),
                    signal="external_task_sensor",
                    file=draft.file,
                    lineno=sensor.lineno,
                )
            )
            continue
        targets = [d for d in drafts if d.dag_id == sensor.external_dag_id and d is not draft]
        if len(targets) != 1:
            unmodeled(f"target DAG {sensor.external_dag_id!r} not (uniquely) in the parse set")
            continue
        target = targets[0]
        if draft.period_s is None or target.period_s is None:
            unmodeled("the period of one of the two DAGs cannot be determined")
            continue
        if draft.period_s != target.period_s:
            unmodeled(
                f"different periods ({target.period_s:.0f}s vs {draft.period_s:.0f}s) "
                "cannot be represented in the single-period model"
            )
            continue
        ratio = sensor.delta_s / draft.period_s
        if ratio < 1 or abs(ratio - round(ratio)) > 1e-9:
            unmodeled(f"execution_delta/T = {ratio:g} is not an integer >= 1")
            continue
        if sensor.external_task_id is None:
            unmodeled("external_task_id missing or not statically resolvable")
            continue
        if sensor.external_task_id not in target.tasks:
            unmodeled(f"target task {sensor.external_task_id!r} not found in the target DAG")
            continue
        edges.append(
            ParsedCrossEdge(
                src=f"{sensor.external_dag_id}.{sensor.external_task_id}",
                dst=sensor.task,
                periods=int(round(ratio)),
                signal="external_task_sensor",
                file=draft.file,
                lineno=sensor.lineno,
            )
        )
    return edges


def _link(drafts: Sequence[_Draft]) -> tuple[ParsedDag, ...]:
    dags: list[ParsedDag] = []
    for draft in drafts:
        cross = _cross_edges(draft, drafts)
        dags.append(
            ParsedDag(
                dag_id=draft.dag_id,
                file=draft.file,
                lineno=draft.lineno,
                schedule_expr=draft.schedule_expr,
                period_s=draft.period_s,
                tasks=tuple(draft.tasks),
                intra=tuple(draft.intra),
                cross=tuple(dict.fromkeys(cross)),
                warnings=tuple(dict.fromkeys(draft.warnings)),
            )
        )
    return tuple(dags)


def _parse_file(
    source: str, path: str, dag_names: frozenset[str]
) -> tuple[list[_Draft], list[Warning_]]:
    try:
        # Fremde Files sind Systemgrenze: ast.parse schreibt fuer krumme Escapes
        # ("\;" in einem bash_command) SyntaxWarnings auf stderr — unterdruecken,
        # der Befund gehoert nicht in den Terminal-Output der CLI.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(source)
    except (SyntaxError, ValueError) as err:  # ValueError: Nullbytes in fremden Files
        lineno = getattr(err, "lineno", None) or 0
        return [], [Warning_("syntax_error", path, lineno, str(err)[:200])]
    parser = _FileParser(tree, path, dag_names)
    parser.collect_scopes()
    parser.walk_module()
    return parser.drafts, list(dict.fromkeys(parser.file_warnings))


def parse_source(source: str, path: str, dag_names: frozenset[str] = DAG_NAMES) -> ParseResult:
    """Ein File parsen. Der Parse-Satz fuer Sensor-Ziele ist das File selbst."""
    drafts, warnings = _parse_file(source, path, dag_names)
    return ParseResult(dags=_link(drafts), warnings=tuple(warnings))


def parse_files(
    files: Sequence[Path], base: Path, dag_names: frozenset[str] = DAG_NAMES
) -> ParseResult:
    """Eine File-Menge als einen Parse-Satz parsen: Sensor-Ziele werden ueber alle
    Files der Menge aufgeloest, Pfade werden relativ zu `base` berichtet."""
    all_drafts: list[_Draft] = []
    all_warnings: list[Warning_] = []
    for path in files:
        rel = path.relative_to(base).as_posix()
        source = path.read_bytes().decode("utf-8", "replace")
        drafts, warnings = _parse_file(source, rel, dag_names)
        all_drafts.extend(drafts)
        all_warnings.extend(warnings)
    return ParseResult(dags=_link(all_drafts), warnings=tuple(all_warnings))


def parse_path(root: Path, dag_names: frozenset[str] = DAG_NAMES) -> ParseResult:
    """Ein File oder ein Verzeichnis parsen. Sensor-Ziele werden ueber alle Files aufgeloest."""
    if root.is_file():
        return parse_files([root], root.parent, dag_names)
    files = [
        p for p in sorted(root.rglob("*.py")) if not (SKIP_DIRS & set(p.parts)) and p.is_file()
    ]
    return parse_files(files, root, dag_names)


def select_dags(result: ParseResult, dag_id: str) -> tuple[ParsedDag, ...]:
    """Der gewaehlte DAG plus transitiv alle DAGs, auf die seine Sensor-Kanten zeigen —
    ohne sie liesse sich die Pipeline nicht bauen (die Kanten-Quelle waere unbekannt)."""
    selected = [dag for dag in result.dags if dag.dag_id == dag_id]
    if not selected:
        return ()
    by_id = {dag.dag_id: dag for dag in result.dags if dag.dag_id is not None}
    while True:
        missing = [
            other
            for edge in (e for dag in selected for e in dag.cross)
            if edge.signal == "external_task_sensor"
            for other_id, other in by_id.items()
            if other not in selected and edge.src.startswith(f"{other_id}.")
        ]
        if not missing:
            return tuple(selected)
        selected.extend(dict.fromkeys(missing))


# --- Struktur + Dauern = Pipeline ----------------------------------------------------


def node_name(dag: ParsedDag, task: str) -> str:
    prefix = dag.dag_id if dag.dag_id is not None else f"{dag.file}:{dag.lineno}"
    return f"{prefix}.{task}"


def to_pipeline(
    dags: Iterable[ParsedDag], durations: float | Mapping[str, float] = 1.0
) -> Pipeline:
    """Heiratet Struktur mit Dauern. Dauern sind nicht Teil von Session 007 (Roadmap 008):
    Default ist einheitlich 1.0 je Task, damit sind Lambda-Aussagen Struktur-Aussagen
    in Einheiten "Tasks auf dem Kreis pro Periode", keine Zeit-Aussagen.

    Task-Knoten sind `dag_id.task_id` namespaced; Sensor-Kanten tragen ihr Ziel bereits
    so und setzen voraus, dass der Ziel-DAG mit uebergeben wird.
    """
    dag_list = list(dags)
    result_durations: dict[str, float] = {}
    intra: list[tuple[str, str]] = []
    cross: list[CrossEdge] = []
    for dag in dag_list:
        for task in dag.tasks:
            node = node_name(dag, task)
            if isinstance(durations, Mapping):
                result_durations[node] = durations[node]
            else:
                result_durations[node] = float(durations)
        intra.extend((node_name(dag, s), node_name(dag, d)) for (s, d) in dag.intra)
        for edge in dag.cross:
            src = edge.src if edge.signal == "external_task_sensor" else node_name(dag, edge.src)
            cross.append(CrossEdge(src=src, dst=node_name(dag, edge.dst), periods=edge.periods))
    return Pipeline(durations=result_durations, intra=intra, cross=cross)
