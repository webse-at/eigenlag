"""Konsistenz Parser <-> Scanner auf den gemeinsamen Fixtures (Spec 007, Vorentscheid 2).

Beide teilen die Signal-Definition aus wiki/signals.md, nicht die Extraktion. Dieser
Test haelt sie deckungsgleich: pro DAG muss die Menge der erkannten Signal-Arten
identisch sein. Laufen sie auseinander, ist einer von beiden falsch, und
wiki/signals.md entscheidet, welcher.

Der Test lebt im Scanner, weil die Abhaengigkeit Produkt <- Scanner laufen muss:
der Scanner darf das Package importieren, nie umgekehrt.
"""

from __future__ import annotations

from pathlib import Path

from eigenlag.parse_airflow import ParsedDag, parse_path
from scanner.analyze import analyze_repo

FIXTURES = Path(__file__).parent / "fixtures" / "repo_airflow"

# Parser-Befunde -> Signal-Art des Scanners. Kanten tragen ihr Signal direkt;
# bewusst nicht modellierte Signale stehen als Warnung mit aequivalenter Art.
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


def test_signal_arten_pro_dag_identisch_auf_den_fixtures() -> None:
    repo = analyze_repo(FIXTURES, repo="fixtures")
    parsed = parse_path(FIXTURES, dag_names=repo.dag_names)

    scanner_by_key = {(d.file, d.lineno): {s.kind for s in d.signals} for d in repo.dags}
    parser_by_key = {(d.file, d.lineno): parser_kinds(d) for d in parsed.dags}

    assert set(scanner_by_key) == set(parser_by_key), (
        "Scanner und Parser finden verschiedene DAGs:\n"
        f"nur Scanner: {sorted(set(scanner_by_key) - set(parser_by_key))}\n"
        f"nur Parser: {sorted(set(parser_by_key) - set(scanner_by_key))}"
    )

    mismatches = {
        key: (scanner_by_key[key], parser_by_key[key])
        for key in scanner_by_key
        if scanner_by_key[key] != parser_by_key[key]
    }
    assert not mismatches, f"Signal-Arten weichen ab: {mismatches}"
