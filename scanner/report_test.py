import csv
import json
from pathlib import Path
from typing import Any

import pytest

from scanner.report import (
    compute,
    dbt_rows,
    demo_share,
    factory_rows,
    is_demo,
    main,
    permalink,
    render,
    result_rows,
    top_examples,
)

Json = dict[str, Any]

SHA = "a" * 40


def record(**overrides: Any) -> Json:
    base: Json = {
        "repo": "acme/etl",
        "queries": ["depends_on_past language:python"],
        "stars": 12,
        "clone_ok": True,
        "sha": SHA,
        "files_parsed": 3,
        "syntax_errors": 0,
        "dags": [],
        "factories": [],
        "dbt": {"projects": 0, "models": 0, "signals": []},
        "errors": [],
    }
    return {**base, **overrides}


def dag(**overrides: Any) -> Json:
    base: Json = {
        "dag_id": "etl",
        "file": "dags/deep/path/etl.py",
        "lineno": 12,
        "schedule_class": "subdaily",
        "schedule_raw": "0 */6 * * *",
        "task_count": 4,
        "signals": [
            {
                "kind": "depends_on_past",
                "file": "dags/deep/path/etl.py",
                "lineno": 17,
                "source": "operator",
                "inferred": False,
            }
        ],
    }
    return {**base, **overrides}


def test_risk_candidate_needs_both_halves() -> None:
    records = [
        record(dags=[dag()]),
        record(repo="acme/daily", dags=[dag(schedule_class="daily_or_slower", schedule_raw=None)]),
        record(repo="acme/quiet", dags=[dag(signals=[])]),
    ]
    rows = {row["repo"]: row for row in result_rows(records)}
    assert (rows["acme/etl"]["has_crossrun"], rows["acme/etl"]["risk_candidate"]) == (1, 1)
    assert (rows["acme/daily"]["has_crossrun"], rows["acme/daily"]["risk_candidate"]) == (1, 0)
    assert (rows["acme/quiet"]["has_crossrun"], rows["acme/quiet"]["risk_candidate"]) == (0, 0)


def signal(kind: str, lineno: int = 17, source: str = "operator") -> Json:
    return {
        "kind": kind,
        "file": "dags/deep/path/etl.py",
        "lineno": lineno,
        "source": source,
        "inferred": False,
    }


def test_g_only_is_its_own_class_and_never_the_core_quote() -> None:
    g_only = dag(signals=[signal("max_active_runs", lineno=12, source="dag_call")])
    row = result_rows([record(dags=[g_only])])[0]
    assert row["sig_g_max_active_runs"] == 1
    # Kern-Quote und has_crossrun bleiben auf A–F bezogen, sonst ist 003 nicht vergleichbar.
    assert (row["has_crossrun"], row["risk_candidate"]) == (0, 0)
    assert row["risk_candidate_g_only"] == 1


def test_g_only_still_needs_a_subdaily_schedule() -> None:
    g_daily = dag(
        schedule_class="daily_or_slower",
        signals=[signal("max_active_runs", lineno=12, source="dag_call")],
    )
    row = result_rows([record(dags=[g_daily])])[0]
    assert (row["risk_candidate"], row["risk_candidate_g_only"]) == (0, 0)


def test_core_signal_plus_g_counts_in_the_core_class() -> None:
    both = dag(
        signals=[
            signal("depends_on_past"),
            signal("max_active_runs", lineno=12, source="dag_call"),
        ]
    )
    row = result_rows([record(dags=[both])])[0]
    assert row["sig_g_max_active_runs"] == 1
    assert (row["risk_candidate"], row["risk_candidate_g_only"]) == (1, 0)


def test_compute_counts_the_two_classes_separately() -> None:
    records = [
        record(dags=[dag()]),
        record(
            repo="acme/serialized",
            dags=[dag(signals=[signal("max_active_runs", lineno=12, source="dag_call")])],
        ),
    ]
    stats = compute(records, [])
    assert (stats.dags_risk, stats.dags_risk_g_only) == (1, 1)


def test_missing_dag_id_is_flagged_not_guessed() -> None:
    row = result_rows([record(dags=[dag(dag_id=None)])])[0]
    assert row["dag_id"] == ""
    assert row["dag_id_missing"] == 1
    stats = compute([record(dags=[dag(dag_id=None), dag()])], [])
    assert stats.dags_without_id == 1


def test_weak_template_does_not_make_a_risk_candidate() -> None:
    weak = dag(
        signals=[
            {
                "kind": "prev_run_date",
                "file": "dags/deep/path/etl.py",
                "lineno": 20,
                "source": "template",
                "inferred": False,
            }
        ]
    )
    row = result_rows([record(dags=[weak])])[0]
    assert row["sig_f_weak_prev_ds"] == 1
    assert (row["has_crossrun"], row["risk_candidate"]) == (0, 0)


def test_permalink_pins_the_commit_sha_and_the_full_path() -> None:
    row = result_rows([record(dags=[dag()])])[0]
    assert row["permalink"] == f"https://github.com/acme/etl/blob/{SHA}/dags/deep/path/etl.py#L12"
    assert row["evidence"] == "depends_on_past=dags/deep/path/etl.py:17"


def test_permalink_is_empty_without_a_sha() -> None:
    assert permalink("acme/etl", None, "dags/etl.py", 12) == ""


def test_permalink_encodes_hash_and_space_in_the_path() -> None:
    # njuxc/PYAM traegt Dateien wie `tests#jobs#test_scheduler_job.py`; ohne Encoding frisst
    # das `#` den Zeilen-Anker und der Beleg ist nicht mehr nachschlagbar (Regel 6).
    link = permalink("acme/etl", SHA, "data/tests#jobs#x.py", 7)
    assert link == f"https://github.com/acme/etl/blob/{SHA}/data/tests%23jobs%23x.py#L7"
    assert permalink("acme/etl", SHA, "exchange rate/init.py", 3).endswith(
        "/exchange%20rate/init.py#L3"
    )


def test_dbt_never_enters_the_airflow_denominator() -> None:
    dbt = record(
        repo="acme/warehouse",
        dags=[],
        dbt={
            "projects": 1,
            "models": 40,
            "signals": [{"path": "models/f.sql", "lineno": 9, "materialized_from": "config_block"}],
        },
    )
    stats = compute([record(dags=[dag()]), dbt], [])
    assert stats.dags == 1  # nur der Airflow-DAG, das dbt-Model zaehlt hier nicht mit
    assert stats.dags_risk == 1
    assert (stats.dbt_models, stats.dbt_incremental, stats.dbt_repos) == (40, 1, 1)
    assert dbt_rows([dbt])[0]["permalink"] == (
        f"https://github.com/acme/warehouse/blob/{SHA}/models/f.sql#L9"
    )


def test_error_kinds_are_counted_by_category() -> None:
    errors: list[Json] = [
        {"repo": "acme/etl", "kind": "unresolved_default_args", "file": "dags/a.py", "lineno": 3},
        {"repo": "acme/etl", "kind": "unresolved_default_args", "file": "dags/b.py", "lineno": 4},
        {"repo": "acme/two", "kind": "ambiguous_task", "file": "dags/c.py", "lineno": 9},
        {"repo": "acme/two", "kind": "clone_failed", "target": "acme/two", "message": "404"},
    ]
    stats = compute([record(dags=[dag()])], errors)
    assert (stats.unresolved_default_args, stats.unresolved_repos) == (2, 1)
    assert (stats.ambiguous_tasks, stats.ambiguous_repos) == (1, 1)
    assert stats.error_kinds["clone_failed"] == 1


def test_factories_are_counted_but_stay_out_of_the_quote() -> None:
    with_factory = record(
        factories=[
            {
                "kind": "depends_on_past",
                "file": "plugins/kafka.py",
                "lineno": 32,
                "source": "factory",
                "inferred": False,
            }
        ]
    )
    stats = compute([with_factory], [])
    assert (stats.factories_signals, stats.factories_repos) == (1, 1)
    assert stats.dags_risk == 0
    assert factory_rows([with_factory])[0]["permalink"].endswith("plugins/kafka.py#L32")


def test_top_examples_take_one_dag_per_repo_and_sort_by_stars() -> None:
    loud = record(repo="acme/loud", stars=900, dags=[dag(dag_id="a"), dag(dag_id="b", lineno=40)])
    quiet = record(repo="acme/quiet", stars=1, dags=[dag(dag_id="c")])
    examples = top_examples([quiet, loud])
    assert [(e["repo"], e["dag_id"]) for e in examples] == [("acme/loud", "a"), ("acme/quiet", "c")]


HARVEST: Json = {
    "queries": {
        "depends_on_past language:python": {"total": 2284, "done": True},
        "prev_start_date_success language:python": {"total": 816, "done": True},
    }
}


def test_demo_code_is_recognised_by_path_and_by_dag_id() -> None:
    assert is_demo({"file": "airflow/example_dags/x.py", "dag_id": "etl"}) is True
    assert is_demo({"file": "dags/etl.py", "dag_id": "example_branch_dop_operator_v3"}) is True
    assert is_demo({"file": "tests/dags/etl.py", "dag_id": "etl"}) is True
    assert is_demo({"file": "dags/etl.py", "dag_id": "etl"}) is False


def test_demo_share_counts_dags_and_risk_candidates_separately() -> None:
    records = [
        record(dags=[dag()]),  # echt aussehender Risiko-Kandidat
        record(repo="acme/lab", dags=[dag(file="dags/example_dags/x.py")]),  # Demo, Risiko
        record(repo="acme/lib", dags=[dag(file="tests/x.py", schedule_class="daily_or_slower")]),
    ]
    assert demo_share(records) == (2, 3, 1, 2)


def test_report_names_every_denominator_and_the_caveats() -> None:
    stats = compute([record(dags=[dag()])], [])
    text = render(
        stats,
        top_examples([record(dags=[dag()])]),
        (0, 1, 0, 1),
        HARVEST,
        [{"reason": "blocklist"}],
    )
    assert "1 der 2 Code-Search-Queries" in text
    assert "2284" in text  # der abgeschnittene Zaehler steht im Report
    assert "Was diese Zahlen nicht sagen" in text
    assert "Untergrenzen" in text
    assert "fork" in text and "archived" in text
    assert "ADR-012" in text
    assert f"https://github.com/acme/etl/blob/{SHA}" in text


def test_report_discloses_the_definition_change_and_the_baseline() -> None:
    stats = compute([record(dags=[dag()])], [])
    text = render(
        stats,
        top_examples([record(dags=[dag()])]),
        (0, 1, 0, 1),
        HARVEST,
        [{"reason": "blocklist"}],
    )
    # Vorher/Nachher gegen Session 003, mit Ursache je Delta (Spec 006, Abschnitt 3).
    assert "51426" in text and "176" in text
    assert "ADR-015" in text and "ADR-016" in text and "ADR-018" in text
    # G-only heisst: Laufzeit-Monitoring reicht dort. Der Satz steht im Report.
    assert "Laufzeit-Monitoring" in text
    # dbt ist aus 003 uebernommen, nicht neu definiert.
    assert "übernommen" in text


def test_main_writes_all_four_artifacts(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "acme__etl.json").write_text(json.dumps(record(dags=[dag()])))
    (tmp_path / "harvest.json").write_text(json.dumps(HARVEST))
    (tmp_path / "errors.jsonl").write_text("")
    (tmp_path / "rejected.jsonl").write_text(json.dumps({"reason": "blocklist"}) + "\n")

    code = main(
        [
            "--state",
            str(state),
            "--errors",
            str(tmp_path / "errors.jsonl"),
            "--harvest",
            str(tmp_path / "harvest.json"),
            "--rejected",
            str(tmp_path / "rejected.jsonl"),
            "--out",
            str(tmp_path / "out"),
        ]
    )
    assert code == 0
    out = tmp_path / "out"
    for name in ("scan_results.csv", "scan_factories.csv", "scan_dbt.csv", "report.md"):
        assert (out / name).exists()
    rows = list(csv.DictReader((out / "scan_results.csv").open()))
    assert rows[0]["dag_id"] == "etl"
    assert rows[0]["task_count"] == "4"
    assert rows[0]["risk_candidate"] == "1"
    assert "stars" not in rows[0]  # Sortier-Hilfe, keine Spalte der Spec


@pytest.mark.parametrize(
    ("schedule_class", "expected"),
    [
        ("subdaily", 1),
        ("daily_or_slower", 0),
        ("none", 0),
        ("dataset_triggered", 0),
        ("unknown", 0),
    ],
)
def test_only_subdaily_counts_as_risk(schedule_class: str, expected: int) -> None:
    row = result_rows([record(dags=[dag(schedule_class=schedule_class)])])[0]
    assert row["risk_candidate"] == expected
