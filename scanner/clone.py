"""Clone-Schicht: flache Clones in einen Disk-Cache.

Ein vorhandener Clone wird nicht neu gezogen (CLAUDE.md, Regel 8: ein Abbruch nach 180 von
200 Repos darf keine 180 Clones kosten). Ein fehlgeschlagener Clone ist erwartbar (Repo privat
geworden, geloescht, Timeout) und wird strukturiert protokolliert, statt den Lauf zu kippen.
"""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

Json = dict[str, Any]

CLONE_TIMEOUT_S = 120
ErrorSink = Callable[[Json], None]


def cache_dir(full_name: str, root: Path) -> Path:
    return root / full_name.replace("/", "__")


def clone_command(url: str, branch: str, target: Path) -> list[str]:
    return [
        "git",
        "clone",
        "--depth",
        "1",
        "--single-branch",
        "--branch",
        branch,
        "--quiet",
        url,
        str(target),
    ]


def _error(kind: str, target: str, message: str) -> Json:
    return {
        "ts": datetime.now(UTC).isoformat(),
        "kind": kind,
        "target": target,
        "message": message[:500],
    }


def ensure_clone(
    full_name: str,
    url: str,
    branch: str,
    root: Path,
    on_error: ErrorSink | None = None,
) -> Path | None:
    """Repo im Cache bereitstellen. None heisst: Clone fehlgeschlagen, protokolliert."""
    target = cache_dir(full_name, root)
    if (target / ".git").exists():
        return target
    if target.exists():
        shutil.rmtree(target)  # Ruine eines abgebrochenen Laufs
    root.mkdir(parents=True, exist_ok=True)

    try:
        proc = subprocess.run(
            clone_command(url, branch, target),
            capture_output=True,
            text=True,
            timeout=CLONE_TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(target, ignore_errors=True)
        if on_error:
            on_error(_error("clone_timeout", full_name, f"{CLONE_TIMEOUT_S}s ueberschritten"))
        return None

    if proc.returncode != 0:
        shutil.rmtree(target, ignore_errors=True)
        if on_error:
            on_error(_error("clone_failed", full_name, proc.stderr.strip() or "git clone != 0"))
        return None
    return target
