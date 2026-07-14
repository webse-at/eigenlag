import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scanner.run import ErrorLog, dbt_projects, pending, read_candidates, run, scan_repo, state_path

Json = dict[str, Any]

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def origin(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Ein echtes Git-Repo aus beiden Fixtures, damit `ensure_clone` wirklich klont."""
    repo = tmp_path_factory.mktemp("origin")
    shutil.copytree(FIXTURES / "repo_airflow", repo, dirs_exist_ok=True)
    shutil.copytree(FIXTURES / "repo_dbt", repo / "warehouse", dirs_exist_ok=True)
    git("init", "--initial-branch", "main", cwd=repo)
    git("config", "user.email", "test@example.org", cwd=repo)
    git("config", "user.name", "test", cwd=repo)
    git("add", "-A", cwd=repo)
    git("commit", "-m", "fixture", cwd=repo)
    return repo


@pytest.fixture(scope="module")
def record(origin: Path, tmp_path_factory: pytest.TempPathFactory) -> Json:
    work = tmp_path_factory.mktemp("scan")
    candidate = {
        "full_name": "acme/fixture",
        "html_url": str(origin),
        "default_branch": "main",
        "matched_queries": ["depends_on_past language:python"],
        "stars": 3,
    }
    return scan_repo(candidate, work / "repos", ErrorLog(work / "scan_errors.jsonl"))


def test_clone_carries_the_commit_sha(record: Json, origin: Path) -> None:
    assert record["clone_ok"] is True
    head = subprocess.run(
        ["git", "-C", str(origin), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    )
    assert record["sha"] == head.stdout.strip()


def test_dags_and_signals_survive_the_json_round_trip(record: Json) -> None:
    dags = {d["dag_id"]: d for d in record["dags"]}
    assert dags["epsilon"]["schedule_class"] == "subdaily"
    assert dags["epsilon"]["task_count"] == 2
    assert [s["kind"] for s in dags["epsilon"]["signals"]] == ["depends_on_past"]
    assert dags["epsilon"]["signals"][0]["file"] == "dags/default_args.py"
    json.dumps(record)  # der Record geht als State-File auf Disk


def test_dbt_project_in_a_subfolder_is_found_and_the_path_keeps_the_prefix(record: Json) -> None:
    dbt = record["dbt"]
    assert dbt["projects"] == 1
    assert all(s["path"].startswith("warehouse/") for s in dbt["signals"])
    assert dbt["models"] > 0
    assert dbt["signals"]


def test_factories_and_errors_are_kept(record: Json) -> None:
    assert [s["kind"] for s in record["factories"]]
    kinds = {e["kind"] for e in record["errors"]}
    assert {"syntax_error", "unresolved_default_args", "ambiguous_task"} <= kinds


def test_a_folder_named_models_without_dbt_project_is_no_dbt_project(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.sql").write_text("select 1")
    assert dbt_projects(tmp_path) == []
    (tmp_path / "dbt_project.yml").write_text("name: p\n")
    assert dbt_projects(tmp_path) == [tmp_path]


def test_failed_clone_is_logged_and_does_not_raise(tmp_path: Path) -> None:
    errors = ErrorLog(tmp_path / "scan_errors.jsonl")
    record = scan_repo(
        {
            "full_name": "acme/missing",
            "html_url": str(tmp_path / "nirgends"),
            "default_branch": "main",
            "matched_queries": [],
        },
        tmp_path / "repos",
        errors,
    )
    assert record["clone_ok"] is False
    assert record["dags"] == []
    logged = [
        json.loads(line) for line in (tmp_path / "scan_errors.jsonl").read_text().splitlines()
    ]
    assert logged[0]["kind"] == "clone_failed"


def test_run_is_resumable(origin: Path, tmp_path: Path) -> None:
    candidates_file = tmp_path / "candidates.jsonl"
    candidate = {
        "full_name": "acme/fixture",
        "html_url": str(origin),
        "default_branch": "main",
        "matched_queries": [],
    }
    candidates_file.write_text(json.dumps(candidate) + "\n")
    candidates = read_candidates(candidates_file)
    state = tmp_path / "state"

    run(candidates, tmp_path / "repos", state, tmp_path / "errors.jsonl", workers=2)
    written = state_path(state, "acme/fixture")
    assert written.exists()
    stamp = written.stat().st_mtime_ns

    assert list(pending(candidates, state)) == []
    run(candidates, tmp_path / "repos", state, tmp_path / "errors.jsonl", workers=2)
    assert written.stat().st_mtime_ns == stamp  # zweiter Lauf ruehrt das State-File nicht an
