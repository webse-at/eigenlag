"""Schedule-Klassifikation eines DAG.

Cron wird nicht am String geraten, sondern gerechnet: die Klasse ergibt sich aus der
kleinsten Distanz zweier aufeinanderfolgender Feuerzeitpunkte in einem Referenzfenster.
Das ist die einzige Methode, die bei Listen (`0 6,18 * * *`), Schritten (`0 */6 * * *`)
und Kombinationen zuverlaessig bleibt. Siehe wiki/signals.md.
"""

from __future__ import annotations

import ast
from datetime import date, timedelta
from functools import lru_cache
from typing import Literal

ScheduleClass = Literal["subdaily", "daily_or_slower", "none", "dataset_triggered", "unknown"]

MINUTES_PER_DAY = 24 * 60

# Fuenf Jahre, damit auch der 29. Februar zweimal vorkommt und ein jaehrlicher Ausdruck
# ueberhaupt eine Distanz hat. Ausdruecke, die im Fenster nie feuern, sind `unknown`.
WINDOW_START = date(2024, 1, 1)
WINDOW_END = date(2028, 12, 31)

PRESETS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

MONTH_NAMES = {
    name: i
    for i, name in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1
    )
}
DOW_NAMES = {name: i for i, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"], 0)}

DATASET_CALLS = {"Dataset", "Asset", "DatasetAlias", "AssetAlias"}

# Reihenfolge der Positionsargumente von datetime.timedelta.
TIMEDELTA_ARGS = ["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"]


def _field(spec: str, lo: int, hi: int, names: dict[str, int]) -> set[int] | None:
    values: set[int] = set()
    for part in spec.split(","):
        body, sep, step_spec = part.partition("/")
        step = 1
        if sep:
            if not step_spec.isdigit() or int(step_spec) == 0:
                return None
            step = int(step_spec)
        if body in ("*", "?"):
            start, end = lo, hi
        else:
            bounds = [_value(b, names) for b in body.split("-")]
            if any(b is None for b in bounds) or len(bounds) > 2:
                return None
            if len(bounds) == 1:
                start = bounds[0]  # type: ignore[assignment]  # None oben ausgeschlossen
                end = hi if step > 1 else start
            else:
                start, end = bounds  # type: ignore[assignment]
        if start < lo or end > hi or start > end:
            return None
        values.update(range(start, end + 1, step))
    return values or None


def _value(token: str, names: dict[str, int]) -> int | None:
    token = token.strip().lower()
    if token in names:
        return names[token]
    if token.isdigit():
        return int(token)
    return None


def _matching_days(
    months: set[int], doms: set[int], dows: set[int], dom_bound: bool, dow_bound: bool
) -> list[date]:
    days: list[date] = []
    day = WINDOW_START
    while day <= WINDOW_END:
        if day.month in months:
            dom_hit = day.day in doms
            dow_hit = day.isoweekday() % 7 in dows
            # Klassische Cron-Semantik: sind beide Felder eingeschraenkt, gilt ODER.
            hit = (dom_hit or dow_hit) if (dom_bound and dow_bound) else (dom_hit and dow_hit)
            if hit:
                days.append(day)
        day += timedelta(days=1)
    return days


@lru_cache(maxsize=4096)
def min_gap_minutes(expr: str) -> int | None:
    """Kleinste Distanz zweier Feuerzeitpunkte in Minuten, oder None bei unlesbarem Cron."""
    fields = expr.split()
    if len(fields) != 5:
        return None
    minutes = _field(fields[0], 0, 59, {})
    hours = _field(fields[1], 0, 23, {})
    doms = _field(fields[2], 1, 31, {})
    months = _field(fields[3], 1, 12, MONTH_NAMES)
    dows = _field(fields[4], 0, 7, DOW_NAMES)
    if minutes is None or hours is None or doms is None or months is None or dows is None:
        return None
    dows = {d % 7 for d in dows}  # 7 und 0 sind beide Sonntag

    days = _matching_days(
        months, doms, dows, fields[2] not in ("*", "?"), fields[4] not in ("*", "?")
    )
    if not days:
        return None

    times = sorted(h * 60 + m for h in hours for m in minutes)
    gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
    for today, following in zip(days, days[1:], strict=False):
        gaps.append((following - today).days * MINUTES_PER_DAY + times[0] - times[-1])
    return min(gaps) if gaps else None


def classify_cron(expr: str) -> ScheduleClass:
    gap = min_gap_minutes(expr)
    if gap is None:
        return "unknown"
    return "subdaily" if gap < MINUTES_PER_DAY else "daily_or_slower"


def period_seconds(text: str) -> float | None:
    """Der Takt T in Sekunden, aus dem Schedule-Ausdruck gerechnet, oder None bei unlesbarem.

    Der Takt ist die kleinste Distanz zweier Feuerzeitpunkte, also die Periode, gegen die
    Lambda zu halten ist. `@hourly` sind 3600 Sekunden, weil `0 * * * *` das ergibt, nicht
    weil irgendwo eine 3600 steht (CLAUDE.md, Regel 2).
    """
    key = text.strip().strip("\"'").lower()
    expr = PRESETS.get(key, key)
    gap = min_gap_minutes(expr)
    return None if gap is None else float(gap) * 60.0


def timedelta_seconds(call: ast.Call) -> float | None:
    parts: dict[str, float] = {}
    for i, arg in enumerate(call.args):
        if i >= len(TIMEDELTA_ARGS) or not isinstance(arg, ast.Constant):
            return None
        if not isinstance(arg.value, int | float) or isinstance(arg.value, bool):
            return None
        parts[TIMEDELTA_ARGS[i]] = float(arg.value)
    for kw in call.keywords:
        if kw.arg not in TIMEDELTA_ARGS or not isinstance(kw.value, ast.Constant):
            return None
        if not isinstance(kw.value.value, int | float) or isinstance(kw.value.value, bool):
            return None
        parts[kw.arg] = float(kw.value.value)
    if not parts:
        return None
    return timedelta(**parts).total_seconds()


def call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _classify_string(text: str) -> ScheduleClass:
    key = text.strip().lower()
    if key in ("@once", ""):
        return "none"
    if key == "@continuous":
        # Feuert, sobald der Vorlauf fertig ist: die Periode liegt per Definition unter einem Tag.
        return "subdaily"
    if key in PRESETS:
        return classify_cron(PRESETS[key])
    return classify_cron(text.strip())


def classify_node(node: ast.expr | None) -> tuple[ScheduleClass, str | None]:
    """Klasse und der unveraenderte Ausdruck, wie er im DAG-File steht (fuer den Beleg)."""
    if node is None:
        return "none", None
    raw = ast.unparse(node)

    if isinstance(node, ast.Constant):
        if node.value is None:
            return "none", raw
        if isinstance(node.value, str):
            return _classify_string(node.value), raw
        return "unknown", raw

    if isinstance(node, ast.Call):
        name = call_name(node)
        if name in DATASET_CALLS:
            return "dataset_triggered", raw
        if name == "timedelta":
            seconds = timedelta_seconds(node)
            if seconds is None or seconds <= 0:
                return "unknown", raw
            return ("subdaily" if seconds < 24 * 3600 else "daily_or_slower"), raw
        return "unknown", raw

    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        elements = list(node.elts)
        if elements and all(
            isinstance(e, ast.Call) and call_name(e) in DATASET_CALLS for e in elements
        ):
            return "dataset_triggered", raw
        return "unknown", raw

    return "unknown", raw
