"""Sensor-Nachlauf (Spec 008, Auftrag 5): die 14 sensor_not_modeled-Faelle aus 007
mit Grund "Ziel nicht im Parse-Satz" — die betroffenen Repos als GANZE Repos parsen
(alle Python-Files, nicht nur die Kandidaten-Files) und je Fall entscheiden:

  modellierbar    — der Sensor traegt jetzt eine Kante (Ziel gefunden, gleiches T,
                    delta/T ganzzahlig); periods > 1 waere der erste Wildbahn-Beleg
                    der ADR-006-Mechanik
  weiterhin_nicht — Ziel im Repo, aber ein anderer Grund verhindert die Kante
                    (steht woertlich in der Parser-Warnung)
  ziel_nicht_im_repo — der Ziel-DAG existiert im ganzen Repo nicht

Artefakte: scan/008_sensor/nachlauf.csv, jede Zeile mit Permalink (Regel 6).
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

from eigenlag.parse_airflow import ParsedDag, parse_files
from scanner.analyze import python_files
from scanner.report import permalink

ROOT = Path(__file__).resolve().parent.parent
REPOS = ROOT / "data" / "repos"
STATE = ROOT / "data" / "scan_state_v2"
WARNINGS_007 = ROOT / "scan" / "007_parse" / "warnings.jsonl"
OUT = ROOT / "scan" / "008_sensor"

TARGET_RE = re.compile(r"Ziel-DAG '([^']*)'")


def load_cases() -> list[dict[str, Any]]:
    cases = []
    with WARNINGS_007.open() as fh:
        for line in fh:
            w = json.loads(line)
            if w["kind"] == "sensor_not_modeled" and "nicht (eindeutig) im Parse-Satz" in w.get(
                "detail", ""
            ):
                match = TARGET_RE.search(w["detail"])
                assert match is not None, w
                w["ziel_dag_id"] = match.group(1)
                cases.append(w)
    return cases


def repo_sha(repo: str) -> str | None:
    state_file = STATE / f"{repo.replace('/', '__')}.json"
    if not state_file.exists():
        return None
    sha: str | None = json.loads(state_file.read_text()).get("sha")
    return sha


def judge(case: dict[str, Any], dags: tuple[ParsedDag, ...]) -> dict[str, Any]:
    source = [d for d in dags if d.dag_id == case["dag_id"] and d.file == case["file"]]
    targets = [d for d in dags if d.dag_id == case["ziel_dag_id"]]
    ziel_ohne_quelle = [
        d for d in targets if not (d.dag_id == case["dag_id"] and d.file == case["file"])
    ]

    row: dict[str, Any] = {
        "repo": case["repo"],
        "dag_id": case["dag_id"],
        "file": case["file"],
        "lineno": case["lineno"],
        "ziel_dag_id": case["ziel_dag_id"],
        "ziel_gefunden": bool(ziel_ohne_quelle),
        "ziel_file": ziel_ohne_quelle[0].file if len(ziel_ohne_quelle) == 1 else "",
        "quell_T_s": source[0].period_s if source else None,
        "ziel_T_s": ziel_ohne_quelle[0].period_s if len(ziel_ohne_quelle) == 1 else None,
        "periods": "",
        "topf": "",
        "grund": "",
        "permalink": permalink(case["repo"], repo_sha(case["repo"]), case["file"], case["lineno"]),
    }
    if not source:
        row["topf"] = "weiterhin_nicht"
        row["grund"] = "Quell-DAG im Ganz-Repo-Parse nicht wiedergefunden"
        return row

    if case["ziel_dag_id"] == case["dag_id"]:
        # Das Ziel existiert — es ist der Quell-DAG selbst. Der Parser modelliert nur
        # Fremd-DAG-Sensoren (drafts ohne den eigenen); Befund fuer den Orchestrator,
        # keine neue Signal-Regel in dieser Session (Spec 008, Zaun).
        row["ziel_gefunden"] = True
        row["ziel_file"] = source[0].file
        row["ziel_T_s"] = source[0].period_s
        row["topf"] = "weiterhin_nicht"
        row["grund"] = "Selbst-Referenz: Sensor zeigt auf den eigenen DAG"
        return row

    edge = [
        e
        for e in source[0].cross
        if e.signal == "external_task_sensor" and e.lineno == case["lineno"]
    ]
    if edge:
        row["topf"] = "modellierbar"
        row["periods"] = edge[0].periods
        row["grund"] = f"Kante {edge[0].src} -> {edge[0].dst}, periods={edge[0].periods}"
        return row

    reasons = [
        w.detail
        for w in source[0].warnings
        if w.kind in ("sensor_not_modeled", "sensor_dynamic_offset") and w.lineno == case["lineno"]
    ]
    if not ziel_ohne_quelle:
        row["topf"] = "ziel_nicht_im_repo"
        row["grund"] = "; ".join(reasons) or "Ziel-DAG existiert im ganzen Repo nicht"
    else:
        row["topf"] = "weiterhin_nicht"
        row["grund"] = "; ".join(reasons)
    return row


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    cases = load_cases()
    print(f"{len(cases)} Faelle aus 007")

    rows = []
    for case in cases:
        repo_dir = REPOS / case["repo"].replace("/", "__")
        files = python_files(repo_dir)
        result = parse_files(files, repo_dir)
        rows.append(judge(case, result.dags))
        print(
            f"{case['repo']}: {len(files)} Files -> {rows[-1]['topf']}"
            f" ({rows[-1]['grund'] or 'ok'})"
        )

    with (OUT / "nachlauf.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    toepfe: dict[str, int] = {}
    for row in rows:
        toepfe[row["topf"]] = toepfe.get(row["topf"], 0) + 1
    print("\nToepfe:", toepfe)
    print(f"geschrieben: {OUT / 'nachlauf.csv'} ({len(rows)} Zeilen)")


if __name__ == "__main__":
    main()
