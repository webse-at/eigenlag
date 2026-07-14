"""CSV und Report aus den State-Files des Scan-Laufs.

Jede Zahl im Report hat einen Nenner, und jeder Treffer hat einen Permalink auf den
Commit-SHA des Clones (CLAUDE.md, Regel 6). Airflow und dbt werden getrennt ausgewertet:
ein dbt-Model hat keinen Schedule, kann die Risiko-Bedingung also nie erfuellen, und wuerde
den Airflow-Nenner nur verduennen (ADR-012).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

Json = dict[str, Any]  # State-Files sind Ein-/Ausgabe-Schema, kein Modell.

STRONG = {
    "depends_on_past",
    "wait_for_downstream",
    "external_task_sensor",
    "include_prior_dates",
    "prev_run_success",
}
WEAK = {"prev_run_date"}

SIGNAL_COLUMNS = {
    "sig_a_depends_on_past": "depends_on_past",
    "sig_b_wait_for_downstream": "wait_for_downstream",
    "sig_c_ext_sensor_delta": "external_task_sensor",
    "sig_d_include_prior_dates": "include_prior_dates",
    "sig_f_prev_success_tmpl": "prev_run_success",
    "sig_f_weak_prev_ds": "prev_run_date",
}

RESULT_FIELDS = [
    "repo",
    "file",
    "dag_id",
    "dag_lineno",
    "schedule_raw",
    "schedule_class",
    "task_count",
    *SIGNAL_COLUMNS,
    "has_crossrun",
    "risk_candidate",
    "evidence",
    "permalink",
]


def load_records(state_dir: Path) -> list[Json]:
    return [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(state_dir.glob("*.json"))
    ]


def load_errors(path: Path) -> list[Json]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def permalink(repo: str, sha: str | None, file: str, lineno: int) -> str:
    if not sha:
        return ""
    return f"https://github.com/{repo}/blob/{sha}/{file}#L{lineno}"


def evidence(signals: list[Json]) -> str:
    return ";".join(f"{s['kind']}={s['file']}:{s['lineno']}" for s in signals)


def result_rows(records: list[Json]) -> list[Json]:
    rows: list[Json] = []
    for record in records:
        for dag in record["dags"]:
            kinds = {s["kind"] for s in dag["signals"]}
            has_crossrun = bool(kinds & STRONG)
            rows.append(
                {
                    "repo": record["repo"],
                    "file": dag["file"],
                    "dag_id": dag["dag_id"] or "",
                    "dag_lineno": dag["lineno"],
                    "schedule_raw": dag["schedule_raw"] or "",
                    "schedule_class": dag["schedule_class"],
                    "task_count": dag["task_count"],
                    **{col: int(kind in kinds) for col, kind in SIGNAL_COLUMNS.items()},
                    "has_crossrun": int(has_crossrun),
                    "risk_candidate": int(has_crossrun and dag["schedule_class"] == "subdaily"),
                    "evidence": evidence(dag["signals"]),
                    "permalink": permalink(
                        record["repo"], record["sha"], dag["file"], dag["lineno"]
                    ),
                    "stars": record.get("stars") or 0,
                }
            )
    return rows


def factory_rows(records: list[Json]) -> list[Json]:
    return [
        {
            "repo": record["repo"],
            "file": signal["file"],
            "lineno": signal["lineno"],
            "signal": signal["kind"],
            "permalink": permalink(record["repo"], record["sha"], signal["file"], signal["lineno"]),
        }
        for record in records
        for signal in record["factories"]
    ]


def dbt_rows(records: list[Json]) -> list[Json]:
    return [
        {
            "repo": record["repo"],
            "model": signal["path"],
            "lineno": signal["lineno"],
            "materialized": "incremental",
            "materialized_from": signal["materialized_from"],
            "permalink": permalink(record["repo"], record["sha"], signal["path"], signal["lineno"]),
        }
        for record in records
        for signal in record["dbt"]["signals"]
    ]


def write_csv(path: Path, rows: list[Json], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


@dataclass
class Stats:
    repos_total: int = 0
    repos_cloned: int = 0
    repos_failed: int = 0
    files_parsed: int = 0
    syntax_errors: int = 0
    dags: int = 0
    dags_crossrun: int = 0
    dags_subdaily: int = 0
    dags_risk: int = 0
    repos_with_dags: int = 0
    repos_with_risk: int = 0
    signal_dags: Counter[str] = field(default_factory=Counter)
    schedule_classes: Counter[str] = field(default_factory=Counter)
    error_kinds: Counter[str] = field(default_factory=Counter)
    factories_signals: int = 0
    factories_repos: int = 0
    unresolved_default_args: int = 0
    unresolved_repos: int = 0
    ambiguous_tasks: int = 0
    ambiguous_repos: int = 0
    dbt_repos: int = 0
    dbt_models: int = 0
    dbt_incremental: int = 0
    dbt_repos_incremental: int = 0
    dbt_materialized_from: Counter[str] = field(default_factory=Counter)


def pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.1f} %" if whole else "—"


def compute(records: list[Json], errors: list[Json]) -> Stats:
    stats = Stats(repos_total=len(records))
    rows = result_rows(records)

    for record in records:
        stats.repos_cloned += int(record["clone_ok"])
        stats.repos_failed += int(not record["clone_ok"])
        stats.files_parsed += record["files_parsed"]
        stats.syntax_errors += record["syntax_errors"]
        stats.repos_with_dags += int(bool(record["dags"]))
        if record["factories"]:
            stats.factories_repos += 1
            stats.factories_signals += len(record["factories"])
        if record["dbt"]["projects"]:
            stats.dbt_repos += 1
        stats.dbt_models += record["dbt"]["models"]
        stats.dbt_incremental += len(record["dbt"]["signals"])
        if record["dbt"]["signals"]:
            stats.dbt_repos_incremental += 1
        for signal in record["dbt"]["signals"]:
            stats.dbt_materialized_from[signal["materialized_from"]] += 1

    risk_repos: set[str] = set()
    for row in rows:
        stats.dags += 1
        stats.schedule_classes[row["schedule_class"]] += 1
        stats.dags_crossrun += row["has_crossrun"]
        stats.dags_subdaily += int(row["schedule_class"] == "subdaily")
        stats.dags_risk += row["risk_candidate"]
        if row["risk_candidate"]:
            risk_repos.add(row["repo"])
        for col in SIGNAL_COLUMNS:
            stats.signal_dags[col] += row[col]
    stats.repos_with_risk = len(risk_repos)

    unresolved_repos: set[str] = set()
    ambiguous_repos: set[str] = set()
    for error in errors:
        # Die Harvest-Phase (Session 001) hat HTTP-Fehler ohne `kind` in dieselbe Datei geschrieben.
        kind = error.get("kind", "github_api_error")
        stats.error_kinds[kind] += 1
        if kind == "unresolved_default_args":
            stats.unresolved_default_args += 1
            unresolved_repos.add(error.get("repo", ""))
        if kind == "ambiguous_task":
            stats.ambiguous_tasks += 1
            ambiguous_repos.add(error.get("repo", ""))
    stats.unresolved_repos = len(unresolved_repos)
    stats.ambiguous_repos = len(ambiguous_repos)
    return stats


# Pfad-Segmente, an denen ein DAG mit hoher Wahrscheinlichkeit Anschauungsmaterial ist und
# keine betriebene Pipeline. Heuristik, bewusst grob: sie korrigiert keine Zahl, sie beziffert
# nur, wie stark der Nenner (und der Zaehler) von Demo-Code getragen wird.
DEMO_MARKERS = (
    "example",
    "test",
    "tutorial",
    "docs",
    "sample",
    "demo",
    "practica",
    "practice",
    "course",
    "learn",
)


def is_demo(row: Json) -> bool:
    path = row["file"].lower()
    dag_id = str(row["dag_id"]).lower()
    return any(marker in path for marker in DEMO_MARKERS) or dag_id.startswith(
        ("example_", "tutorial")
    )


def demo_share(records: list[Json]) -> tuple[int, int, int, int]:
    """(Demo-DAGs, DAGs gesamt, Demo-Risiko-Kandidaten, Risiko-Kandidaten gesamt)."""
    rows = result_rows(records)
    risk = [row for row in rows if row["risk_candidate"]]
    return (
        sum(1 for row in rows if is_demo(row)),
        len(rows),
        sum(1 for row in risk if is_demo(row)),
        len(risk),
    )


def top_examples(records: list[Json], limit: int = 10) -> list[Json]:
    risk = [row for row in result_rows(records) if row["risk_candidate"]]
    by_repo: dict[str, Json] = {}
    for row in sorted(risk, key=lambda r: (-r["stars"], r["repo"])):
        by_repo.setdefault(row["repo"], row)  # ein Beispiel je Repo, sonst dominiert ein Repo
    return list(by_repo.values())[:limit]


def signal_list(row: Json) -> str:
    return ", ".join(col.split("_", 2)[2] for col in SIGNAL_COLUMNS if row[col]) or "—"


def render(
    stats: Stats,
    examples: list[Json],
    demo: tuple[int, int, int, int],
    harvest: Json,
    rejected: list[Json],
) -> str:
    capped = {q: e["total"] for q, e in harvest["queries"].items() if (e["total"] or 0) > 1000}
    reject_reasons = Counter(r.get("reason", "?") for r in rejected)
    demo_dags, all_dags, demo_risk, all_risk = demo
    lines: list[str] = []
    add = lines.append

    add("# Scan-Report — Cross-Run-Abhängigkeiten in öffentlichen Airflow- und dbt-Repos")
    add("")
    add(
        f"Lauf über {stats.repos_total} Kandidaten-Repos aus `data/candidates.jsonl`. "
        "Alle Zahlen mit Nenner, jeder Treffer mit Permalink auf den Commit-SHA des Clones."
    )
    add("")
    add("## Airflow")
    add("")
    add("### Lauf")
    add("")
    add("| Kennzahl | Wert |")
    add("|---|---|")
    add(f"| Repos in der Kandidatenliste | {stats.repos_total} |")
    add(f"| davon geklont | {stats.repos_cloned} ({pct(stats.repos_cloned, stats.repos_total)}) |")
    add(f"| Clone fehlgeschlagen | {stats.repos_failed} |")
    add(f"| Python-Files geparst | {stats.files_parsed} |")
    add(f"| davon `SyntaxError` (protokolliert, kein Abbruch) | {stats.syntax_errors} |")
    add(f"| Repos mit mindestens einem DAG | {stats.repos_with_dags} |")
    add("")
    add("Fehler-Kategorien aus `data/scan_errors.jsonl`:")
    add("")
    add("| Kategorie | Vorkommen |")
    add("|---|---|")
    for kind, count in stats.error_kinds.most_common():
        add(f"| `{kind}` | {count} |")
    add("")
    add("### Befund")
    add("")
    add(f"**Nenner ist der DAG, nicht das Repo und nicht das File: {stats.dags} DAGs.**")
    add("")
    add("| Kennzahl | Absolut | Anteil an allen DAGs |")
    add("|---|---|---|")
    add(
        f"| DAGs mit Cross-Run-Kante (starkes Signal) | {stats.dags_crossrun} | "
        f"{pct(stats.dags_crossrun, stats.dags)} |"
    )
    add(
        f"| DAGs mit sub-täglichem Schedule | {stats.dags_subdaily} | "
        f"{pct(stats.dags_subdaily, stats.dags)} |"
    )
    add(
        f"| **Risiko-Kandidaten (Cross-Run **und** sub-täglich im selben DAG)** | "
        f"**{stats.dags_risk}** | **{pct(stats.dags_risk, stats.dags)}** |"
    )
    add(f"| Repos mit mindestens einem Risiko-Kandidaten | {stats.repos_with_risk} | — |")
    add("")
    add("Schedule-Klassen:")
    add("")
    add("| Klasse | DAGs | Anteil |")
    add("|---|---|---|")
    for name, count in stats.schedule_classes.most_common():
        add(f"| `{name}` | {count} | {pct(count, stats.dags)} |")
    add("")
    add("Signale, je DAG gezählt (ein DAG kann mehrere tragen):")
    add("")
    add("| Signal | DAGs | Anteil | In der Quote |")
    add("|---|---|---|---|")
    for col, kind in SIGNAL_COLUMNS.items():
        count = stats.signal_dags[col]
        strength = "ja" if kind in STRONG else "nein (ADR-005)"
        add(f"| `{col}` | {count} | {pct(count, stats.dags)} | {strength} |")
    add("")
    add("### Beispiele (Risiko-Kandidaten, ein DAG je Repo, nach Sternen sortiert)")
    add("")
    if not examples:
        add("Keine.")
    else:
        add("| Repo | DAG | Schedule | Signale | Beleg |")
        add("|---|---|---|---|---|")
        for row in examples:
            add(
                f"| `{row['repo']}` ({row['stars']}★) | `{row['dag_id'] or '—'}` | "
                f"`{row['schedule_raw'] or '—'}` | {signal_list(row)} | "
                f"[{row['file']}:{row['dag_lineno']}]({row['permalink']}) |"
            )
    add("")
    add("## dbt")
    add("")
    add(
        "Getrennt ausgewertet, mit eigenem Nenner. Ein dbt-Model enthält keinen Schedule: "
        "wie oft es läuft, steht in Airflow, in dbt Cloud oder in einem Cron außerhalb des "
        "Repos. Die Risiko-Bedingung (starkes Signal **und** sub-täglich im selben DAG) ist "
        "hier konstruktionsbedingt nicht auswertbar, deshalb taucht kein dbt-Model in der "
        "Airflow-Quote auf (ADR-012)."
    )
    add("")
    add("| Kennzahl | Wert |")
    add("|---|---|")
    add(f"| Repos mit `dbt_project.yml` | {stats.dbt_repos} |")
    add(f"| Models gefunden | {stats.dbt_models} |")
    add(
        f"| Models mit echter Selbst-Kante (`materialized='incremental'` **und** "
        f"`is_incremental()`) | {stats.dbt_incremental} "
        f"({pct(stats.dbt_incremental, stats.dbt_models)}) |"
    )
    add(f"| Repos mit mindestens einem solchen Model | {stats.dbt_repos_incremental} |")
    add("")
    add("Woher die Materialisierung kam:")
    add("")
    add("| Quelle | Models |")
    add("|---|---|")
    for source, count in stats.dbt_materialized_from.most_common():
        add(f"| `{source}` | {count} |")
    add("")
    add(
        "**Bei dbt kennen wir den Kreis, aber nicht den Takt.** Genau deshalb ist ein Werkzeug "
        "nötig, das beides zusammenbringt."
    )
    add("")
    add("## Untergrenzen: wo die Quote zu klein ist")
    add("")
    add(
        "Alle drei Posten zeigen in dieselbe Richtung: der Scanner findet weniger, als da ist. "
        "Keiner davon wird in den Zähler gerechnet."
    )
    add("")
    add("| Posten | Zahl | Warum nicht in der Quote |")
    add("|---|---|---|")
    add(
        f"| Task-Factories (ADR-009) | {stats.factories_signals} Signale in "
        f"{stats.factories_repos} Repos | Ohne interprozedurale Analyse keinem DAG zuzuordnen. "
        "Die Signale sind echt, siehe `scan_factories.csv`. |"
    )
    add(
        f"| `unresolved_default_args` | {stats.unresolved_default_args} Fälle in "
        f"{stats.unresolved_repos} Repos | `default_args` aus Import oder dynamischer "
        "Konstruktion. Ein `depends_on_past=True` darin ist unsichtbar, wird aber nicht geraten. |"
    )
    add(
        f"| Ambige Tasks | {stats.ambiguous_tasks} Fälle in {stats.ambiguous_repos} Repos | "
        "Operator ohne DAG-Bindung in einem File mit mehreren DAGs. Wird nicht geraten. |"
    )
    add("")
    add("## Was diese Zahlen nicht sagen")
    add("")
    add(
        f"- **Der 1000er-Deckel.** {len(capped)} der {len(harvest['queries'])} Code-Search-Queries "
        "laufen in das Limit der GitHub-Code-Search von 1000 ausgelieferten Ergebnissen: "
        + ", ".join(f"`{q.split(' language')[0]}` meldet {t} Treffer" for q, t in capped.items())
        + ". Geholt wurden je 1000. Die Stichprobe ist nach oben abgeschnitten."
    )
    add(
        "- **Die Stichprobe ist keine Zufallsauswahl** aus allen Airflow-Nutzern, sondern aus "
        "öffentlichen Repos, die bestimmte Begriffe enthalten und über die Code-Search "
        "auffindbar sind."
    )
    add(
        "- **Öffentliche Repos sind keine Produktions-Pipelines, und das ist hier keine "
        f"Floskel.** {demo_dags} der {all_dags} DAGs ({pct(demo_dags, all_dags)}) liegen in "
        "einem Pfad mit `example`, `test`, `tutorial`, `docs` oder `sample`, oder tragen eine "
        f"`dag_id`, die mit `example_` beginnt. Unter den {all_risk} Risiko-Kandidaten sind es "
        f"{demo_risk} ({pct(demo_risk, all_risk)}). Die Stichprobe zeigt, woran das liegt: der "
        "Airflow-eigene Beispiel-DAG `example_branch_dop_operator_v3` trägt "
        "`depends_on_past=True` bei `*/1 * * * *` und wird in jedes zweite Lern-Repo kopiert. "
        "Die Marker sind eine grobe Heuristik, sie korrigieren keine Zahl. Sie beziffern, wie "
        "stark Zähler und Nenner von Anschauungsmaterial getragen werden, und dieser Anteil ist "
        "der wichtigste Vorbehalt gegen jede Aussage über den Markt."
    )
    add(
        "- **`fork` und `archived` haben null Mal gegriffen** (an 20 Kandidaten nachgeprüft, "
        "Session 001). Die klassische Code-Search liefert diese Repos offenbar nicht aus. Kein "
        "Filter-Fehler, aber ohne Erklärung liest es sich wie einer."
    )
    add(
        f"- **Die Blocklist verwirft {reject_reasons['blocklist']} Repos**, der Größenfilter "
        f"weitere {reject_reasons['size']}, zusammen {sum(reject_reasons.values())}. Jede "
        "Verwerfung steht mit Grund in `data/rejected.jsonl` und ist damit anfechtbar."
    )
    add(
        "- **Der Scanner sagt nicht, dass diese Pipelines instabil sind.** Er sagt, dass sie die "
        "Struktur haben, in der Instabilität entstehen kann, und dass kein Werkzeug ihnen zeigt, "
        "ob sie es sind."
    )
    add("")
    add("## Die Aussage, die nicht an der Prozentzahl hängt")
    add("")
    add(
        f"> Wir haben {stats.dags_crossrun} Airflow-DAGs mit einem Kreis über die Zeitachse "
        f"gefunden, dazu {stats.dbt_incremental} dbt-Models mit einer Selbst-Kante. Für keinen "
        "einzigen davon ist bekannt, wo seine Taktgrenze liegt, weil kein Werkzeug sie ausrechnet."
    )
    add("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CSV und report.md aus dem Scan-Lauf")
    parser.add_argument("--state", type=Path, default=Path("data/scan_state"))
    parser.add_argument("--errors", type=Path, default=Path("data/scan_errors.jsonl"))
    parser.add_argument("--harvest", type=Path, default=Path("data/harvest_state.json"))
    parser.add_argument("--rejected", type=Path, default=Path("data/rejected.jsonl"))
    # data/ ist Cache und Rohdaten und bleibt ungetrackt. Die Artefakte des Laufs sind der
    # Beleg fuer den Report und muessen im Repo liegen.
    parser.add_argument("--out", type=Path, default=Path("scan"))
    args = parser.parse_args(argv)

    records = load_records(args.state)
    errors = load_errors(args.errors)
    harvest = json.loads(args.harvest.read_text(encoding="utf-8"))
    rejected = load_errors(args.rejected)

    write_csv(args.out / "scan_results.csv", result_rows(records), RESULT_FIELDS)
    write_csv(
        args.out / "scan_factories.csv",
        factory_rows(records),
        ["repo", "file", "lineno", "signal", "permalink"],
    )
    write_csv(
        args.out / "scan_dbt.csv",
        dbt_rows(records),
        ["repo", "model", "lineno", "materialized", "materialized_from", "permalink"],
    )
    stats = compute(records, errors)
    report = render(stats, top_examples(records), demo_share(records), harvest, rejected)
    (args.out / "report.md").write_text(report, encoding="utf-8")

    print(
        f"{stats.dags} DAGs, {stats.dags_crossrun} mit Cross-Run, {stats.dags_subdaily} "
        f"sub-täglich, {stats.dags_risk} Risiko-Kandidaten; "
        f"{stats.dbt_incremental} dbt-Models mit Selbst-Kante"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
