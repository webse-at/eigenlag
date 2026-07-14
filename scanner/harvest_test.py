import pytest

from scanner.harvest import (
    AIRFLOW_QUERIES,
    DBT_QUERIES,
    MAX_PAGES,
    MAX_SIZE_KB,
    PER_PAGE,
    Candidate,
    Hit,
    advance,
    merge_hits,
    reject_reason,
)


def repo(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "full_name": "acme/etl",
        "html_url": "https://github.com/acme/etl",
        "default_branch": "main",
        "size": 4210,
        "stargazers_count": 128,
        "pushed_at": "2026-03-11T09:00:00Z",
        "fork": False,
        "archived": False,
        "description": "Production data platform",
    }
    base.update(over)
    return base


@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        (repo(), None),
        (repo(fork=True), "fork"),
        (repo(archived=True), "archived"),
        (repo(size=MAX_SIZE_KB), "size"),
        (repo(size=MAX_SIZE_KB - 1), None),
        (repo(size=900_000), "size"),
        (repo(full_name="acme/awesome-airflow"), "blocklist"),
        (repo(full_name="acme/Airflow-Tutorial"), "blocklist"),
        (repo(full_name="acme/etl", description="A demo of dbt"), "blocklist"),
        (repo(full_name="acme/etl", description=None), None),
        (repo(full_name="acme/dbt-starter-kit"), "blocklist"),
        (repo(full_name="acme/etl", description="Course material"), "blocklist"),
        (repo(full_name="acme/etl", description="We test our pipelines"), None),
        (repo(full_name="acme/latest-pipelines"), None),
        (repo(full_name="acme/pipeline-templates"), "blocklist"),
        (repo(full_name="acme/etl", description="Contains a sandbox env"), "blocklist"),
    ],
)
def test_reject_reason(meta: dict[str, object], expected: str | None) -> None:
    assert reject_reason(meta) == expected


def test_reject_reason_order_is_stable() -> None:
    assert reject_reason(repo(fork=True, archived=True, size=900_000)) == "fork"


def test_merge_hits_dedups_and_collects_queries_and_paths() -> None:
    hits = [
        Hit(query="depends_on_past language:python", full_name="acme/etl", path="dags/etl.py"),
        Hit(query="depends_on_past language:python", full_name="acme/etl", path="dags/ml.py"),
        Hit(query="wait_for_downstream language:python", full_name="acme/etl", path="dags/etl.py"),
        Hit(query=DBT_QUERIES[0], full_name="beta/wh", path="models/f.sql"),
    ]
    merged = merge_hits(hits)

    assert list(merged) == ["acme/etl", "beta/wh"]
    assert merged["acme/etl"] == (
        ["depends_on_past language:python", "wait_for_downstream language:python"],
        ["dags/etl.py", "dags/ml.py"],
    )
    assert merged["beta/wh"] == (
        ["is_incremental language:sql path:models"],
        ["models/f.sql"],
    )


def test_merge_hits_dedups_identical_hit() -> None:
    hits = [
        Hit(query="depends_on_past language:python", full_name="acme/etl", path="dags/etl.py"),
        Hit(query="depends_on_past language:python", full_name="acme/etl", path="dags/etl.py"),
    ]
    assert merge_hits(hits)["acme/etl"] == (["depends_on_past language:python"], ["dags/etl.py"])


def test_candidate_from_meta_uses_search_fields_not_guesses() -> None:
    cand = Candidate.build(
        repo(),
        matched_queries=["depends_on_past language:python"],
        matched_paths=["dags/etl.py"],
    )
    assert cand.as_dict() == {
        "full_name": "acme/etl",
        "html_url": "https://github.com/acme/etl",
        "default_branch": "main",
        "size_kb": 4210,
        "stars": 128,
        "pushed_at": "2026-03-11T09:00:00Z",
        "matched_queries": ["depends_on_past language:python"],
        "matched_paths": ["dags/etl.py"],
    }


@pytest.mark.parametrize(
    ("page", "n_items", "next_page", "done"),
    [
        (1, PER_PAGE, 2, False),
        (7, 52, 8, True),  # Teilseite ist die letzte Seite der Query
        (7, 0, 8, True),
        (MAX_PAGES, PER_PAGE, MAX_PAGES + 1, True),  # 1000er-Deckel der Code-Search
        (MAX_PAGES - 1, PER_PAGE, MAX_PAGES, False),
    ],
)
def test_advance(page: int, n_items: int, next_page: int, done: bool) -> None:
    entry: dict[str, object] = {"next_page": page, "done": False, "total": 999}
    advance(entry, n_items)
    assert entry["next_page"] == next_page
    assert entry["done"] is done


def test_queries_are_the_ones_from_the_spec() -> None:
    assert AIRFLOW_QUERIES == [
        "depends_on_past language:python",
        "wait_for_downstream language:python",
        "ExternalTaskSensor execution_delta language:python",
        "include_prior_dates language:python",
        "prev_start_date_success language:python",
    ]
    assert DBT_QUERIES == ["is_incremental language:sql path:models"]
