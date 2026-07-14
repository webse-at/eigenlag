"""dbt-Analyse: Signal E, `materialized='incremental'` UND `is_incremental()` im Body.

SQL wird nicht geparst, das waere fuer eine Marktzahl unverhaeltnismaessig. Textsuche ist hier
vertretbar, aber erst nach dem Entfernen der Kommentare: ein `is_incremental()` in einem
`--`-, `/* */`- oder `{# #}`-Kommentar ist toter Text, kein Signal. Die Zeilenumbrueche bleiben
beim Entfernen stehen, sonst verschieben sich die Zeilennummern der Belege (CLAUDE.md, Regel 6).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

Json = dict[str, Any]  # dbt-YAML ist fremdes Schema, Systemgrenze.

CONFIG_BLOCK = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.DOTALL)
MATERIALIZED = re.compile(r"materialized\s*=\s*['\"](?P<value>\w+)['\"]")
IS_INCREMENTAL = re.compile(r"\bis_incremental\s*\(\s*\)")
INCREMENTAL = "incremental"


@dataclass(frozen=True)
class DbtSignal:
    path: str
    lineno: int
    materialized_from: str  # config_block | schema_yml | dbt_project
    kind: str = "dbt_incremental"


@dataclass
class DbtAnalysis:
    repo: str
    signals: list[DbtSignal] = field(default_factory=list)
    models: int = 0
    errors: list[Json] = field(default_factory=list)


def strip_sql_comments(sql: str) -> str:
    out: list[str] = []
    i = 0
    quote: str | None = None
    while i < len(sql):
        char = sql[i]
        if quote:
            out.append(char)
            if char == quote:
                quote = None
            i += 1
            continue
        if char in "'\"":
            quote = char
            out.append(char)
            i += 1
            continue
        if sql.startswith("--", i):
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        for opener, closer in (("/*", "*/"), ("{#", "#}")):
            if sql.startswith(opener, i):
                end = sql.find(closer, i + len(opener))
                block = sql[i:] if end == -1 else sql[i : end + len(closer)]
                out.append("\n" * block.count("\n"))
                i += len(block)
                break
        else:
            out.append(char)
            i += 1
    return "".join(out)


def materialized_from_config_block(sql: str) -> str | None:
    block = CONFIG_BLOCK.search(sql)
    if not block:
        return None
    found = MATERIALIZED.search(block.group(0))
    return found.group("value") if found else None


def _flatten_project_config(node: Json, prefix: tuple[str, ...]) -> dict[tuple[str, ...], str]:
    """`models:` aus dbt_project.yml zu Pfad-Praefix -> materialized verdichten."""
    found: dict[tuple[str, ...], str] = {}
    for key, value in node.items():
        if key == "+materialized" and isinstance(value, str):
            found[prefix] = value
        elif isinstance(value, dict) and not key.startswith("+"):
            found.update(_flatten_project_config(value, (*prefix, key)))
    return found


def _project_materialized(root: Path, errors: list[Json]) -> tuple[dict[tuple[str, ...], str], str]:
    path = root / "dbt_project.yml"
    if not path.exists():
        return {}, "models"
    data = _load_yaml(path, root, errors)
    if not isinstance(data, dict):
        return {}, "models"
    model_paths = data.get("model-paths") or ["models"]
    models = data.get("models")
    if not isinstance(models, dict):
        return {}, str(model_paths[0])
    flat: dict[tuple[str, ...], str] = {}
    for project_key, tree in models.items():
        if project_key.startswith("+"):
            continue
        if isinstance(tree, dict):
            flat.update(_flatten_project_config(tree, ()))
    return flat, str(model_paths[0])


def _load_yaml(path: Path, root: Path, errors: list[Json]) -> object:
    try:
        return yaml.safe_load(path.read_bytes().decode("utf-8", "replace"))
    except yaml.YAMLError as err:  # fremdes Repo, kaputtes YAML ist erwartbar
        errors.append(
            {
                "kind": "yaml_error",
                "file": path.relative_to(root).as_posix(),
                "message": str(err)[:200],
            }
        )
        return None


def _schema_materialized(root: Path, errors: list[Json]) -> dict[tuple[str, str], str]:
    """(Ordner, Model-Name) -> materialized aus den `.yml`-Dateien neben den Models."""
    found: dict[tuple[str, str], str] = {}
    for path in sorted(root.rglob("*.yml")):
        # In freier Wildbahn gibt es Verzeichnisse, die auf .yml enden (Swagatd/gcphandson).
        if path.name == "dbt_project.yml" or ".git" in path.parts or not path.is_file():
            continue
        data = _load_yaml(path, root, errors)
        if not isinstance(data, dict):
            continue
        models = data.get("models")
        if not isinstance(models, list):
            continue
        folder = path.parent.relative_to(root).as_posix()
        for entry in models:
            if not isinstance(entry, dict):
                continue
            config = entry.get("config")
            name = entry.get("name")
            if isinstance(config, dict) and isinstance(name, str):
                value = config.get("materialized")
                if isinstance(value, str):
                    found[(folder, name)] = value
    return found


def _project_lookup(
    flat: dict[tuple[str, ...], str], rel_parts: tuple[str, ...]
) -> tuple[str, str] | None:
    """Laengster passender Praefix gewinnt, wie in dbt."""
    best: tuple[str, str] | None = None
    best_len = -1
    for prefix, value in flat.items():
        if rel_parts[: len(prefix)] == prefix and len(prefix) > best_len:
            best, best_len = (value, "dbt_project"), len(prefix)
    return best


def analyze_dbt_repo(root: Path, repo: str) -> DbtAnalysis:
    result = DbtAnalysis(repo=repo)
    flat, model_dir = _project_materialized(root, result.errors)
    schema = _schema_materialized(root, result.errors)

    for path in sorted((root / model_dir).rglob("*.sql")) if (root / model_dir).is_dir() else []:
        if ".git" in path.parts or not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        result.models += 1
        raw = path.read_bytes().decode("utf-8", "replace")
        sql = strip_sql_comments(raw)

        materialized: tuple[str, str] | None = None
        block = materialized_from_config_block(sql)
        if block:
            materialized = (block, "config_block")
        else:
            key = (path.parent.relative_to(root).as_posix(), path.stem)
            if key in schema:
                materialized = (schema[key], "schema_yml")
            else:
                inside = path.parent.relative_to(root / model_dir).parts
                materialized = _project_lookup(flat, inside)

        if not materialized or materialized[0] != INCREMENTAL:
            continue
        hit = _first_line(sql, IS_INCREMENTAL)
        if hit is None:
            continue
        result.signals.append(DbtSignal(path=rel, lineno=hit, materialized_from=materialized[1]))

    return result


def _first_line(sql: str, pattern: re.Pattern[str]) -> int | None:
    for number, line in enumerate(sql.splitlines(), 1):
        if pattern.search(line):
            return number
    return None
