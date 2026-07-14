"""Der Wikimedia-Fall: Code lesen, Laufzeiten messen, Lambda rechnen.

Die Zahlen dieses Moduls stehen in `wikimedia/case.md`. Jede kommt aus einer PromQL im
Cache (`data/wikimedia/cache/`) oder aus dem Scanner ueber den geklonten DAG-Code, keine
ist von Hand eingetragen.

Aufruf: `python -m wikimedia.case` (liest den Cache, holt nur Fehlendes nach).
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eigenlag.maxplus import condense, drift, howard
from eigenlag.model import CrossEdge, Pipeline
from eigenlag.schedule import period_seconds
from scanner.analyze import RepoAnalysis, analyze_repo
from wikimedia.fetch import Fetcher, Json
from wikimedia.runs import Run, Window, extract_runs, samples, stats, windows

REPO = Path("data/wikimedia/airflow-dags")
OUT_JSON = Path("data/wikimedia/case_numbers.json")
OUT_CSV = Path("wikimedia/wikimedia_dags.csv")

# Festes Fenster, damit jeder Lauf dieselbe Antwort aus dem Cache liest. Das Ende ist die
# Tagesgrenze nach dem letzten Messtag, das Fenster reicht also bis zur Abfrage.
END = 1_784_073_600  # 2026-07-15T00:00:00Z, Beginn also 2026-06-15T00:00:00Z
WINDOW = "30d"

CASE_DAGS = ("wcqs_streaming_updater_reconcile_hourly", "wdqs_streaming_updater_reconcile_hourly")
DURATION = 'airflow_dagrun_duration{{dag_id="{dag}",state="success"}}[{window}]'
DELAY = 'airflow_dagrun_schedule_delay{{dag_id="{dag}"}}[{window}]'


def lambda_of(duration: float) -> float:
    """Lambda der Pipeline auf DAG-Ebene: ein Lauf, eine Kante auf sich selbst.

    `depends_on_past=True` haengt jeden Task an seinen Vorlauf, `max_active_runs=1` haengt
    den ganzen Lauf an den vorigen. Auf DAG-Ebene fallen beide auf dieselbe Kante zusammen:
    Lauf k kann nicht beginnen, bevor Lauf k-1 fertig ist. Der Kreis hat ein Gewicht, und
    das ist die Laufzeit.
    """
    pipeline = Pipeline(
        durations={"dagrun": duration},
        intra=[],
        cross=[CrossEdge(src="dagrun", dst="dagrun", periods=1)],
    )
    graph, _paths = condense(pipeline)
    result = howard(graph)
    assert result is not None, "eine Selbst-Kante ist ein Kreis, howard muss ihn finden"
    lam, _cycle = result
    return lam


@dataclass(frozen=True)
class Measurement:
    dag_id: str
    runs: list[Run]
    clean: list[Window]

    @property
    def duration(self) -> Any:
        return stats([run.duration for run in self.runs])

    @property
    def cadence(self) -> float | None:
        """Beobachteter Takt: ueber alle luecken-freien Fenster, nach Laufzahl gewichtet."""
        spans = [(w.span, len(w.runs) - 1) for w in self.clean if len(w.runs) > 1]
        if not spans:
            return None
        return sum(s for s, _ in spans) / sum(n for _, n in spans)


def measure(fetcher: Fetcher, dag_id: str) -> Measurement:
    series = fetcher.query(DURATION.format(dag=dag_id, window=WINDOW), at=END)
    runs = extract_runs(series)
    return Measurement(dag_id=dag_id, runs=runs, clean=windows(runs))


def lateness(fetcher: Fetcher, dag_id: str, runs: list[Run]) -> list[tuple[float, float]]:
    """Je Lauf: (Verspaetung beim Start, Laufzeit).

    `airflow_dagrun_schedule_delay` wird beim Start eines Laufs gesetzt und bleibt stehen.
    Der Startzeitpunkt ist `Ende - Dauer`, der passende Wert ist der erste Sample danach.
    """
    points = samples(fetcher.query(DELAY.format(dag=dag_id, window=WINDOW), at=END))
    pairs: list[tuple[float, float]] = []
    for run in runs:
        start = run.at - run.duration
        after = [(t, v) for t, v in points if start <= t <= start + 300]
        if after:
            pairs.append((after[0][1] / 1000.0, run.duration))
    return pairs


def scan() -> RepoAnalysis:
    return analyze_repo(REPO, "wikimedia/airflow-dags")


def _takt(finding: Any) -> float | None:
    return period_seconds(finding.schedule_expr) if finding.schedule_expr else None


def sweep(fetcher: Fetcher, analysis: RepoAnalysis) -> list[dict[str, Any]]:
    """Alle DAGs der Organisation: geplanter Takt aus dem Code, gelieferter Takt aus der Metrik.

    Je **Instanz**, nicht je `dag_id`: Wikimedia betreibt neun Airflow-Instanzen, und manche
    DAGs liegen in mehreren. `maintenance_cleanup_airflow_db` laeuft in dreizehn Namespaces.
    Ein `sum by (dag_id)` haette daraus 323 taegliche Laeufe in 30 Tagen gemacht, also
    dreizehn Instanzen als elf zusaetzliche Laeufe pro Tag gezaehlt.

    Triage ueber `changes()`: die Gauge wechselt genau dann den Wert, wenn ein Lauf endet.
    Serverseitig gerechnet, damit nicht 400 Abfragen noetig sind. Was auffaellt, wird danach
    an den Rohsamples nachgerechnet.
    """
    key = "(dag_id, kubernetes_namespace)"
    changes = fetcher.query(
        f'sum by {key} (changes(airflow_dagrun_duration{{state="success"}}[{WINDOW}]))', at=END
    )
    median = fetcher.query(
        f"max by {key} (quantile_over_time(0.5, "
        f'airflow_dagrun_duration{{state="success"}}[{WINDOW}]))',
        at=END,
    )

    def ident(entry: Json) -> tuple[str, str]:
        return entry["metric"]["dag_id"], entry["metric"]["kubernetes_namespace"]

    counted = {ident(s): float(s["value"][1]) for s in changes}
    typical = {ident(s): float(s["value"][1]) / 1000.0 for s in median}

    from_code = {d.dag_id: d for d in analysis.dags if d.dag_id}
    keys = set(counted) | {(dag_id, "") for dag_id in from_code}
    rows: list[dict[str, Any]] = []
    for dag_id, namespace in sorted(keys):
        if namespace == "" and any(d == dag_id and ns for d, ns in counted):
            continue  # laeuft, also steht die Zeile schon mit ihrer Instanz da
        finding = from_code.get(dag_id)
        takt = _takt(finding) if finding else None
        n = counted.get((dag_id, namespace))
        rows.append(
            {
                "dag_id": dag_id,
                "instanz": namespace,
                "im_code": finding is not None,
                "laeuft": (dag_id, namespace) in counted,
                "schedule": (finding.schedule_expr if finding else "") or "",
                "takt_s": takt,
                "laeufe_30d": int(n) if n else 0,
                "median_dauer_s": (
                    round(typical[(dag_id, namespace)], 1)
                    if (dag_id, namespace) in typical
                    else None
                ),
                "signale": ";".join(sorted({s.kind for s in finding.signals})) if finding else "",
                "beleg": f"{finding.file}:{finding.lineno}" if finding else "",
            }
        )
    return rows


def overrun(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """DAGs, deren mittlere Laufzeit ueber ihrem geplanten Takt liegt."""
    return [
        row
        for row in rows
        if row["takt_s"] and row["median_dauer_s"] and row["median_dauer_s"] > row["takt_s"]
    ]


def main() -> None:
    fetcher = Fetcher()
    analysis = scan()
    report: dict[str, Any] = {
        "fenster": {"ende": END, "spanne": WINDOW},
        "scanner": {
            "dags": len(analysis.dags),
            "mit_dag_id": sum(1 for d in analysis.dags if d.dag_id),
            "mit_signal": sum(1 for d in analysis.dags if d.signals),
            "risiko": len(analysis.risk_candidates),
            "konstruktoren": sorted(analysis.dag_names),
        },
        "faelle": {},
    }

    for dag_id in CASE_DAGS:
        measurement = measure(fetcher, dag_id)
        if not measurement.runs:
            report["faelle"][dag_id] = {"fehler": "keine Laeufe im Fenster"}
            continue
        duration = measurement.duration
        finding = next(
            (d for d in analysis.dags if d.file.endswith("rdf_streaming_updater_reconcile.py")),
            None,
        )
        takt = _takt(finding) if finding else None
        assert takt, "der Takt muss aus dem Code kommen"

        pairs = lateness(fetcher, dag_id, measurement.runs)
        report["faelle"][dag_id] = {
            "takt_s": takt,
            "laeufe": duration.n,
            "dauer_s": {
                "median": round(duration.median, 1),
                "mittel": round(duration.mean, 1),
                "p95": round(duration.p95, 1),
                "min": round(duration.minimum, 1),
                "max": round(duration.maximum, 1),
            },
            "lambda_s": {
                "median": round(lambda_of(duration.median), 1),
                "mittel": round(lambda_of(duration.mean), 1),
                "p95": round(lambda_of(duration.p95), 1),
            },
            "drift_s_pro_lauf": {
                "median": round(drift(lambda_of(duration.median), takt), 1),
                "mittel": round(drift(lambda_of(duration.mean), takt), 1),
                "p95": round(drift(lambda_of(duration.p95), takt), 1),
            },
            "beobachteter_takt_s": (round(measurement.cadence, 1) if measurement.cadence else None),
            "fenster_ohne_luecke": [
                {"von": int(w.start), "bis": int(w.end), "laeufe": len(w.runs)}
                for w in measurement.clean
            ],
            "letzter_lauf": int(measurement.runs[-1].at),
            "verspaetung_s": {
                "median": round(statistics.median([p for p, _ in pairs]), 1) if pairs else None,
                "korrelation_mit_dauer": (
                    round(statistics.correlation([p for p, _ in pairs], [d for _, d in pairs]), 3)
                    if len(pairs) > 2
                    else None
                ),
                "paare": len(pairs),
            },
        }

    rows = sweep(fetcher, analysis)
    ueber = sorted(overrun(rows), key=lambda r: -r["median_dauer_s"] / r["takt_s"])
    report["organisation"] = {
        "zeilen": len(rows),
        "dags_im_code": sum(1 for r in rows if r["im_code"]),
        "dags_mit_laufzeit": sum(1 for r in rows if r["laeuft"]),
        "beides": sum(1 for r in rows if r["laeuft"] and r["im_code"]),
        "takt_bekannt": sum(1 for r in rows if r["takt_s"]),
        "laufzeit_ueber_takt": len(ueber),
        "laufzeit_ueber_takt_mit_kreis": [
            {
                "dag_id": r["dag_id"],
                "instanz": r["instanz"],
                "takt_s": r["takt_s"],
                "median_dauer_s": r["median_dauer_s"],
                "signale": r["signale"],
                "beleg": r["beleg"],
            }
            for r in ueber
            if r["signale"]
        ],
        "laufzeit_ueber_takt_ohne_kreis": [r["dag_id"] for r in ueber if not r["signale"]],
        "mehr_laeufe_als_der_takt_erlaubt": [
            {"dag_id": r["dag_id"], "instanz": r["instanz"], "laeufe_30d": r["laeufe_30d"]}
            for r in rows
            if r["takt_s"]
            and r["laeufe_30d"] > 1.5 * (30 * 86400 / r["takt_s"])  # 50 % Luft fuer Nachholer
        ],
    }

    OUT_CSV.write_text(
        "dag_id,instanz,im_code,laeuft,schedule,takt_s,laeufe_30d,median_dauer_s,signale,beleg\n"
        + "\n".join(
            f"{r['dag_id']},{r['instanz']},{int(r['im_code'])},{int(r['laeuft'])},"
            f'"{r["schedule"]}",{r["takt_s"] or ""},{r["laeufe_30d"]},'
            f"{r['median_dauer_s'] or ''},{r['signale']},{r['beleg']}"
            for r in rows
        )
        + "\n",
        encoding="utf-8",
    )
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    print(f"\nRequests: {fetcher.requests_made}, aus dem Cache: {fetcher.cache_hits}")


if __name__ == "__main__":
    main()
