from pathlib import Path

import pytest

from scanner.analyze_dbt import (
    DbtAnalysis,
    analyze_dbt_repo,
    materialized_from_config_block,
    strip_sql_comments,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "repo_dbt"


@pytest.fixture(scope="module")
def result() -> DbtAnalysis:
    return analyze_dbt_repo(FIXTURE, "acme/fixture-dbt")


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("select 1 -- is_incremental()", "select 1 "),
        ("select 1\n-- is_incremental()\nfrom t", "select 1\n\nfrom t"),
        ("/* is_incremental() */select 1", "select 1"),
        # Zeilenumbrueche bleiben stehen, sonst verschieben sich die Zeilennummern der Belege.
        ("/*\nis_incremental()\n*/\nselect 1", "\n\n\nselect 1"),
        ("{# is_incremental() #}select 1", "select 1"),
        ("select '-- kein Kommentar'", "select '-- kein Kommentar'"),
        ('select "/* auch nicht */"', 'select "/* auch nicht */"'),
    ],
)
def test_strip_sql_comments(sql: str, expected: str) -> None:
    assert strip_sql_comments(sql) == expected


def test_materialized_from_config_block() -> None:
    assert (
        materialized_from_config_block("{{ config(materialized='incremental') }}") == "incremental"
    )
    assert materialized_from_config_block('{{ config(materialized="view") }}') == "view"
    assert materialized_from_config_block("select 1") is None


def test_only_incremental_plus_is_incremental_counts(result: DbtAnalysis) -> None:
    assert sorted((m.path, m.lineno) for m in result.signals) == [
        ("models/marts/orders.sql", 6),
        ("models/staging/sessions.sql", 6),
        ("models/staging/stg_events.sql", 8),
    ]


def test_incremental_without_is_incremental_is_no_signal(result: DbtAnalysis) -> None:
    assert "models/marts/customers.sql" not in {m.path for m in result.signals}


def test_is_incremental_only_in_a_comment_is_no_signal(result: DbtAnalysis) -> None:
    paths = {m.path for m in result.signals}
    assert "models/marts/legacy.sql" not in paths
    assert "models/staging/stg_users.sql" not in paths


def test_config_block_beats_dbt_project_yml(result: DbtAnalysis) -> None:
    # staging ist per dbt_project.yml eine View, stg_events setzt im Model incremental.
    assert "models/staging/stg_events.sql" in {m.path for m in result.signals}


def test_schema_yml_beats_dbt_project_yml(result: DbtAnalysis) -> None:
    assert "models/staging/sessions.sql" in {m.path for m in result.signals}


def test_model_count_is_reported(result: DbtAnalysis) -> None:
    assert result.models == 6


def test_a_directory_named_like_a_yaml_file_does_not_crash(tmp_path: Path) -> None:
    # Aus dem Lauf ueber echte Repos: Swagatd/gcphandson hat ein Verzeichnis `..._models.yml`.
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "schema.yml").mkdir()
    (tmp_path / "dbt_project.yml").write_text(
        "name: acme\nmodels:\n  acme:\n    +materialized: view\n"
    )
    assert analyze_dbt_repo(tmp_path, "acme/weird").signals == []
