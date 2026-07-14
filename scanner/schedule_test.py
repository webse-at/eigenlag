import ast

import pytest

from scanner.schedule import ScheduleClass, classify_node, min_gap_minutes, period_seconds


def node(expr: str) -> ast.expr:
    return ast.parse(expr, mode="eval").body


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ('"@hourly"', "subdaily"),
        ('"@daily"', "daily_or_slower"),
        ('"@weekly"', "daily_or_slower"),
        ('"@monthly"', "daily_or_slower"),
        ('"@yearly"', "daily_or_slower"),
        ('"@once"', "none"),
        ('"*/15 * * * *"', "subdaily"),
        ('"0 */6 * * *"', "subdaily"),
        ('"0 6,18 * * *"', "subdaily"),
        ('"0 3 * * *"', "daily_or_slower"),
        ('"30 4 * * 1"', "daily_or_slower"),
        ('"0 0 1 * *"', "daily_or_slower"),
        ('"*/30 9-17 * * MON-FRI"', "subdaily"),
        ("timedelta(hours=4)", "subdaily"),
        ("timedelta(days=1)", "daily_or_slower"),
        ("timedelta(minutes=90)", "subdaily"),
        ("timedelta(days=1, hours=12)", "daily_or_slower"),
        ("datetime.timedelta(hours=6)", "subdaily"),
        ("None", "none"),
        ('[Dataset("s3://bucket/key")]', "dataset_triggered"),
        ('Dataset("s3://bucket/key")', "dataset_triggered"),
        ('[Asset("s3://bucket/key")]', "dataset_triggered"),
        ('"kaputt"', "unknown"),
        ("SOME_CONSTANT", "unknown"),
        ("EventsTimetable(every=..., restrict_to=...)", "unknown"),
        ("timedelta(hours=stunden)", "unknown"),
    ],
)
def test_classify_node(expr: str, expected: ScheduleClass) -> None:
    assert classify_node(node(expr))[0] == expected


def test_missing_schedule_kwarg_is_none() -> None:
    assert classify_node(None) == ("none", None)


def test_raw_expression_is_kept_for_the_report() -> None:
    assert classify_node(node('"0 */6 * * *"'))[1] == "'0 */6 * * *'"
    assert classify_node(node("timedelta(hours=4)"))[1] == "timedelta(hours=4)"


@pytest.mark.parametrize(
    ("cron", "gap"),
    [
        ("*/15 * * * *", 15),
        ("0 * * * *", 60),
        ("0 */6 * * *", 360),
        ("0 6,18 * * *", 720),
        ("0 3 * * *", 1440),
        ("30 4 * * 1", 7 * 1440),
        ("0 0 1 * *", 28 * 1440),
        ("0 12 * * MON,WED", 2 * 1440),
    ],
)
def test_min_gap_minutes(cron: str, gap: int) -> None:
    assert min_gap_minutes(cron) == gap


def test_dom_and_dow_are_or_semantics() -> None:
    # Klassische Cron-Regel: sind Tag-des-Monats und Wochentag beide gesetzt,
    # feuert der Ausdruck, wenn EINES von beiden passt. Der 1. und jeder Montag.
    assert min_gap_minutes("0 12 1 * MON") is not None


def test_cron_that_never_fires_is_unknown() -> None:
    assert min_gap_minutes("0 12 30 2 *") is None
    assert classify_node(node('"0 12 30 2 *"'))[0] == "unknown"


def test_period_seconds_rechnet_den_takt_aus_dem_ausdruck() -> None:
    assert period_seconds("@hourly") == 3600.0
    assert period_seconds("'@hourly'") == 3600.0  # so, wie ast.unparse ihn liefert
    assert period_seconds("@daily") == 86400.0
    assert period_seconds("0 */6 * * *") == 6 * 3600.0
    assert period_seconds("*/15 * * * *") == 900.0
    assert period_seconds("0 6,18 * * *") == 12 * 3600.0  # kleinste Distanz, nicht die groesste
    assert period_seconds("@once") is None
    assert period_seconds("kein cron") is None
