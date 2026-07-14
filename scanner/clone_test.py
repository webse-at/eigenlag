import subprocess
from pathlib import Path

import pytest

from scanner.clone import CLONE_TIMEOUT_S, cache_dir, clone_command, ensure_clone


def test_cache_dir_is_flat_and_collision_free(tmp_path: Path) -> None:
    assert cache_dir("acme/etl", tmp_path) == tmp_path / "acme__etl"
    assert cache_dir("acme/etl", tmp_path) != cache_dir("acme-etl/x", tmp_path)


def test_clone_command_is_shallow_and_single_branch(tmp_path: Path) -> None:
    cmd = clone_command("https://github.com/acme/etl", "main", tmp_path / "acme__etl")
    assert cmd[:2] == ["git", "clone"]
    assert "--depth" in cmd and "1" in cmd
    assert "--single-branch" in cmd
    assert "--branch" in cmd and "main" in cmd
    assert cmd[-1] == str(tmp_path / "acme__etl")


def test_timeout_is_two_minutes() -> None:
    assert CLONE_TIMEOUT_S == 120


def git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    repo = tmp_path / "origin"
    (repo / "dags").mkdir(parents=True)
    (repo / "dags" / "one.py").write_text("x = 1\n")
    git("init", "-b", "main", cwd=repo)
    git("-c", "user.email=t@e.st", "-c", "user.name=t", "add", ".", cwd=repo)
    git("-c", "user.email=t@e.st", "-c", "user.name=t", "commit", "-m", "init", cwd=repo)
    return repo


def test_clone_lands_on_disk(tmp_path: Path, origin: Path) -> None:
    cache = tmp_path / "repos"
    path = ensure_clone("acme/etl", origin.as_uri(), "main", cache)
    assert path is not None
    assert (path / "dags" / "one.py").read_text() == "x = 1\n"


def test_existing_clone_is_not_pulled_again(tmp_path: Path, origin: Path) -> None:
    cache = tmp_path / "repos"
    first = ensure_clone("acme/etl", origin.as_uri(), "main", cache)
    assert first is not None
    (first / "marker").write_text("nicht ueberschreiben")

    second = ensure_clone("acme/etl", origin.as_uri(), "main", cache)
    assert second == first
    assert (first / "marker").exists()


def test_failed_clone_returns_none_and_leaves_no_ruin(tmp_path: Path) -> None:
    cache = tmp_path / "repos"
    errors: list[dict[str, object]] = []
    path = ensure_clone(
        "acme/gone",
        (tmp_path / "does-not-exist").as_uri(),
        "main",
        cache,
        on_error=errors.append,
    )
    assert path is None
    assert not (cache / "acme__gone").exists()
    assert len(errors) == 1
    assert errors[0]["target"] == "acme/gone"
    assert errors[0]["kind"] == "clone_failed"
