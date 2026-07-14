"""Korpus-Validierung des Airflow-Parsers (Spec 007, Auftrag 3 und 4).

Laeuft ueber die DAG-Files der Kern- und G-only-Kandidaten aus scan/v2/scan_results.csv
(Clones unter data/repos/, nichts wird neu geklont) und misst:

1. Parse-Quote: Files, DAGs, Warnungs-Verteilung — jede Zahl mit Nenner.
2. Konsistenz Parser <-> Scanner: Signal-Arten pro DAG, Abweichungen einzeln.
3. Kreuzvergleich Karp = Howard (= Brute-Force bei <= 8 Kreis-Knoten) auf jedem
   kondensierten Graphen, Dauer 1.0 je Task (offener Punkt aus Abnahme 004).
4. Teilpfad-Jagd: Kern-Kandidaten mit Lambda < Critical Path bei uniformen Dauern
   (ADR-019) — der Falltyp, den ein Laufzeit-Dashboard nicht beantworten kann.

Lambda-Aussagen hier sind Struktur-Aussagen in Einheiten "Tasks auf dem Kreis pro
Periode", keine Zeit-Aussagen (Spec 007, Vorentscheid 3).
"""

from __future__ import annotations

import csv
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

from eigenlag.crosscheck_test import max_cycle_mean_bruteforce
from eigenlag.maxplus import condense, critical_path, howard, karp
from eigenlag.parse_airflow import ParsedDag, parse_files, to_pipeline
from scanner.analyze import analyze_source, python_files, repo_dag_names

ROOT = Path(__file__).resolve().parent.parent
REPOS = ROOT / "data" / "repos"
SCAN_CSV = ROOT / "scan" / "v2" / "scan_results.csv"
OUT = ROOT / "scan" / "007_parse"

TOL = 1e-9
BRUTE_MAX_NODES = 8

# Parser-Befunde -> Signal-Art des Scanners (gleiche Tabelle wie im Konsistenz-Test).
WARNING_TO_KIND = {
    "sensor_not_modeled": "external_task_sensor",
    "sensor_dynamic_offset": "external_task_sensor",
    "include_prior_dates": "include_prior_dates",
    "prev_run_success": "prev_run_success",
    "prev_run_date": "prev_run_date",
    "depends_on_past": "depends_on_past",
    "wait_for_downstream": "wait_for_downstream",
    "max_active_runs": "max_active_runs",
}


def parser_kinds(dag: ParsedDag) -> set[str]:
    kinds = {edge.signal for edge in dag.cross}
    kinds |= {WARNING_TO_KIND[w.kind] for w in dag.warnings if w.kind in WARNING_TO_KIND}
    return kinds


def _components(dags: list[ParsedDag]) -> list[list[ParsedDag]]:
    """Schwach zusammenhaengende Gruppen ueber Sensor-Kanten (die einzigen DAG-Grenzgaenger)."""
    index = {id(d): i for i, d in enumerate(dags)}
    parent = list(range(len(dags)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(a: int, b: int) -> None:
        parent[find(a)] = find(b)

    by_id: dict[str, list[ParsedDag]] = {}
    for d in dags:
        if d.dag_id is not None:
            by_id.setdefault(d.dag_id, []).append(d)
    for d in dags:
        for edge in d.cross:
            if edge.signal != "external_task_sensor":
                continue
            target_id = edge.src.rsplit(".", 1)[0]
            for target in by_id.get(target_id, []):
                union(index[id(d)], index[id(target)])

    groups: dict[int, list[ParsedDag]] = {}
    for i, d in enumerate(dags):
        groups.setdefault(find(i), []).append(d)
    return list(groups.values())


def main() -> None:
    started = time.monotonic()
    OUT.mkdir(parents=True, exist_ok=True)

    rows = list(csv.DictReader(SCAN_CSV.open()))
    candidates = [
        r for r in rows if r["risk_candidate"] == "1" or r["risk_candidate_g_only"] == "1"
    ]
    by_repo: dict[str, list[dict[str, str]]] = {}
    for r in candidates:
        by_repo.setdefault(r["repo"], []).append(r)
    row_of = {(r["repo"], r["file"], r["dag_lineno"]): r for r in candidates}

    stats: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    warning_records: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    graph_rows: list[dict[str, Any]] = []
    teilpfad_rows: list[dict[str, Any]] = []

    for repo in sorted(by_repo):
        root = REPOS / repo.replace("/", "__")
        if not root.is_dir():
            errors.append({"kind": "repo_missing", "repo": repo})
            continue
        stats["repos"] += 1
        dag_names = repo_dag_names(python_files(root))

        wanted = sorted({r["file"] for r in by_repo[repo]})
        paths = []
        for rel in wanted:
            path = root / rel
            if not path.is_file():
                errors.append({"kind": "file_missing", "repo": repo, "file": rel})
                continue
            paths.append(path)
        stats["files"] += len(paths)

        result = parse_files(paths, root, dag_names)
        stats["dags"] += len(result.dags)
        stats["dags_with_id"] += sum(1 for d in result.dags if d.dag_id is not None)
        for w in result.warnings:
            warning_counts[f"file:{w.kind}"] += 1
            if w.kind == "syntax_error":
                stats["syntax_errors"] += 1
            warning_records.append(
                {
                    "repo": repo,
                    "file": w.file,
                    "lineno": w.lineno,
                    "kind": w.kind,
                    "detail": w.detail,
                }
            )
        for d in result.dags:
            for w in d.warnings:
                warning_counts[w.kind] += 1
                warning_records.append(
                    {
                        "repo": repo,
                        "dag_id": d.dag_id,
                        "file": w.file,
                        "lineno": w.lineno,
                        "kind": w.kind,
                        "detail": w.detail,
                    }
                )

        # Kandidaten-Deckung: findet der Parser die DAG-Zeile aus dem Scan wieder?
        parsed_keys = {(repo, d.file, str(d.lineno)) for d in result.dags}
        for r in by_repo[repo]:
            key = (repo, r["file"], r["dag_lineno"])
            stats["candidate_rows"] += 1
            if key in parsed_keys:
                stats["candidate_rows_found"] += 1

        # Konsistenz Parser <-> Scanner, DAG fuer DAG.
        parser_by_key = {(d.file, d.lineno): parser_kinds(d) for d in result.dags}
        for path in paths:
            rel = path.relative_to(root).as_posix()
            source = path.read_bytes().decode("utf-8", "replace")
            analysis = analyze_source(source, rel, dag_names)
            for finding in analysis.dags:
                scanner_kinds = {s.kind for s in finding.signals}
                parsed = parser_by_key.get((finding.file, finding.lineno))
                if parsed is None:
                    mismatches.append(
                        {
                            "repo": repo,
                            "file": finding.file,
                            "lineno": finding.lineno,
                            "dag_id": finding.dag_id,
                            "kind": "dag_only_scanner",
                            "scanner": sorted(scanner_kinds),
                        }
                    )
                elif parsed != scanner_kinds:
                    mismatches.append(
                        {
                            "repo": repo,
                            "file": finding.file,
                            "lineno": finding.lineno,
                            "dag_id": finding.dag_id,
                            "kind": "kinds_differ",
                            "scanner": sorted(scanner_kinds),
                            "parser": sorted(parsed),
                        }
                    )
            scanner_keys = {(d.file, d.lineno) for d in analysis.dags}
            for d in result.dags:
                if d.file == rel and (d.file, d.lineno) not in scanner_keys:
                    mismatches.append(
                        {
                            "repo": repo,
                            "file": d.file,
                            "lineno": d.lineno,
                            "dag_id": d.dag_id,
                            "kind": "dag_only_parser",
                            "parser": sorted(parser_kinds(d)),
                        }
                    )

        # Kreuzvergleich und Teilpfad-Jagd je Komponente.
        for component in _components(list(result.dags)):
            try:
                pipeline = to_pipeline(component)
            except ValueError as err:
                errors.append(
                    {
                        "kind": "pipeline_invalid",
                        "repo": repo,
                        "files": sorted({d.file for d in component}),
                        "message": str(err)[:200],
                    }
                )
                continue
            graph, paths_map = condense(pipeline)
            lam_karp = karp(graph)
            result_howard = howard(graph)
            lam_howard = None if result_howard is None else result_howard[0]
            lam_brute = (
                max_cycle_mean_bruteforce(graph) if len(graph.nodes) <= BRUTE_MAX_NODES else None
            )
            agree = _same(lam_karp, lam_howard) and (
                len(graph.nodes) > BRUTE_MAX_NODES or _same(lam_karp, lam_brute)
            )
            stats["graphs"] += 1
            stats["graphs_agree"] += int(agree)
            stats["graphs_bruteforced"] += int(len(graph.nodes) <= BRUTE_MAX_NODES)
            if lam_karp is not None:
                stats["graphs_with_cycle"] += 1
            cp, cp_path = critical_path(pipeline) if pipeline.durations else (0.0, [])
            graph_rows.append(
                {
                    "repo": repo,
                    "dags": ";".join(sorted(str(d.dag_id) for d in component)),
                    "nodes": len(graph.nodes),
                    "edges": len(graph.edges),
                    "tasks": len(pipeline.durations),
                    "karp": lam_karp,
                    "howard": lam_howard,
                    "brute": lam_brute,
                    "agree": int(agree),
                    "critical_path": cp,
                }
            )
            if not agree:
                errors.append(
                    {
                        "kind": "eigenvalue_disagreement",
                        "repo": repo,
                        "karp": lam_karp,
                        "howard": lam_howard,
                        "brute": lam_brute,
                    }
                )

            # Teilpfad: nur Kern-Kandidaten, Lambda < Critical Path, strukturell.
            core_rows = [
                row_of[(repo, d.file, str(d.lineno))]
                for d in component
                if (repo, d.file, str(d.lineno)) in row_of
                and row_of[(repo, d.file, str(d.lineno))]["risk_candidate"] == "1"
            ]
            if not core_rows or lam_howard is None or result_howard is None:
                continue
            if lam_howard < cp - TOL:
                cycle = result_howard[1]
                cycle_tasks = _resolve_cycle(cycle, paths_map)
                for r in core_rows:
                    teilpfad_rows.append(
                        {
                            "repo": repo,
                            "dag_id": r["dag_id"],
                            "file": r["file"],
                            "lambda_uniform": lam_howard,
                            "critical_path_uniform": cp,
                            "cycle_condensed": " -> ".join(
                                f"{e.src}->{e.dst}(p={e.periods})" for e in cycle
                            ),
                            "cycle_tasks": " -> ".join(cycle_tasks),
                            "cp_tasks": " -> ".join(cp_path),
                            "permalink": r["permalink"],
                        }
                    )

    with (OUT / "warnings.jsonl").open("w") as fh:
        for record in warning_records:
            fh.write(json.dumps(record) + "\n")
    _write_outputs(stats, warning_counts, mismatches, errors, graph_rows, teilpfad_rows)
    print(f"Fertig in {time.monotonic() - started:.0f}s")
    print(json.dumps(stats, indent=2, sort_keys=True))
    print("Warnungen:", json.dumps(dict(warning_counts.most_common()), indent=2))
    print(f"Mismatches: {len(mismatches)}, Fehler: {len(errors)}")
    print(f"Teilpfad-Faelle: {len(teilpfad_rows)}")


def _same(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) < 1e-7


def _resolve_cycle(cycle: list[Any], paths_map: dict[Any, Any]) -> list[str]:
    tasks: list[str] = []
    for edge in cycle:
        segment = paths_map.get((edge.src, edge.dst, edge.periods), ())
        for task in segment:
            if not tasks or tasks[-1] != task:
                tasks.append(task)
    return tasks


def _write_outputs(
    stats: Counter[str],
    warning_counts: Counter[str],
    mismatches: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    graph_rows: list[dict[str, Any]],
    teilpfad_rows: list[dict[str, Any]],
) -> None:
    (OUT / "stats.json").write_text(
        json.dumps(
            {"stats": dict(stats), "warnings": dict(warning_counts.most_common())},
            indent=2,
            sort_keys=True,
        )
    )
    with (OUT / "mismatches.jsonl").open("w") as fh:
        for m in mismatches:
            fh.write(json.dumps(m) + "\n")
    with (OUT / "errors.jsonl").open("w") as fh:
        for e in errors:
            fh.write(json.dumps(e) + "\n")
    for name, rows in (("graph_check.csv", graph_rows), ("teilpfad.csv", teilpfad_rows)):
        if not rows:
            (OUT / name).write_text("")
            continue
        with (OUT / name).open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
