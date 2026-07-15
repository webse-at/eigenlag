"""Der Report (Spec 009): Urteil zuerst, Zahlen danach, Grenzen zum Schluss, aber
pflichtig. Er wird von Data Engineers gelesen, die dem Tool noch nicht trauen und
kein Wiki kennen — jede Behauptung traegt ihre Grundlage bei sich.

compose() baut ein dict mit stabilen Keys (ab Session 010 liest das CI-Gate genau
diese Felder), render() macht daraus den deutschen Text. Eine Quelle fuer beides:
Text und --json koennen nicht auseinanderlaufen.

Der Warnblock und die Modellgrenzen-Fusszeile sind nie abschaltbar. Ein Report, der
seine eigenen Grenzen verschweigt, produziert genau die Fehlalarme, die er anderen
vorwirft (wiki/positioning.md, Zwischenbewertung).
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from eigenlag.analyze import Analysis
from eigenlag.durations import Statistic, TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.model import Pipeline
from eigenlag.montecarlo import MonteCarloResult
from eigenlag.parse_airflow import ParsedDag, node_name

GRENZBAND = 0.10  # |Lambda - T| < 10 % von T heisst "an der Grenze"

MODELLGRENZEN = [
    "Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen"
    " Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'.",
    "Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die"
    " reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig.",
    "Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum"
    " Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan.",
]

SENSOR_KREIS_TEXT = (
    "Die gemessene Dauer eines Sensors enthaelt Wartezeit auf externe Ereignisse und"
    " laesst sich aus der Metadaten-DB nicht von Arbeitszeit trennen. Lambda kann"
    " dadurch ueberschaetzt sein und ist keine harte Untergrenze mehr. Wartet der"
    " Sensor auf Daten der laufenden Periode, koppelt er die Pipeline an die Wanduhr:"
    " solche Systeme pendeln sich genau an ihrer Taktgrenze ein, und die gemessenen"
    " Dauern sind bereits das Ergebnis dieses eingeschwungenen Zustands."
)

F_DIVERGENZ_TEXT = (
    "Hinweis zu prev_*_success: der Zugriff zaehlt als Cross-Run-Befund, erzeugt aber"
    " keine Lambda-Kante. Das Template rendert einen Zeitstempel und wartet nicht;"
    " ein Task damit startet puenktlich und liest schlimmstenfalls veraltete Daten."
    " Das ist ein Korrektheits-, kein Durchsatz-Problem."
)


@dataclass(frozen=True)
class WhatIfTask:
    task: str
    seconds: float


@dataclass(frozen=True)
class WhatIfDropEdge:
    src: str
    dst: str


def resolve_task_name(pipeline: Pipeline, name: str) -> str:
    """Akzeptiert volle Knoten-Namen (dag.task) und blanke Task-Namen, wenn eindeutig."""
    if name in pipeline.durations:
        return name
    matches = [task for task in pipeline.tasks if task.rsplit(".", 1)[-1] == name]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"Task {name!r} nicht gefunden. Tasks: {', '.join(pipeline.tasks)}")
    raise ValueError(f"Task {name!r} ist mehrdeutig: {', '.join(matches)}")


def _lam_of(pipeline: Pipeline) -> float | None:
    outcome = howard(condense(pipeline)[0])
    return None if outcome is None else outcome[0]


def _with_duration(pipeline: Pipeline, task: str, seconds: float) -> Pipeline:
    durations = dict(pipeline.durations)
    durations[task] = seconds
    return Pipeline(durations=durations, intra=pipeline.intra, cross=pipeline.cross)


def _without_edge(pipeline: Pipeline, src: str, dst: str) -> Pipeline:
    remaining = [e for e in pipeline.cross if not (e.src == src and e.dst == dst)]
    if len(remaining) == len(pipeline.cross):
        vorhanden = ", ".join(f"{e.src} -> {e.dst}" for e in pipeline.cross) or "keine"
        raise ValueError(f"Cross-Kante {src} -> {dst} nicht gefunden. Vorhanden: {vorhanden}")
    return Pipeline(durations=pipeline.durations, intra=pipeline.intra, cross=remaining)


def _what_if(
    analysis: Analysis, requested: Sequence[WhatIfTask | WhatIfDropEdge]
) -> list[dict[str, Any]]:
    pipeline = analysis.pipeline
    base = analysis.lam
    scenarios: list[tuple[str, Pipeline, bool]] = []

    if base is not None:
        for task in analysis.cycle_tasks:
            halved = pipeline.durations[task] / 2
            scenarios.append(
                (
                    f"Task {task} halbiert (auf {_num(halved)} s)",
                    _with_duration(pipeline, task, halved),
                    False,
                )
            )
        seen: set[tuple[str, str]] = set()
        for edge in pipeline.cross:
            if (edge.src, edge.dst) in seen:
                continue
            seen.add((edge.src, edge.dst))
            scenarios.append(
                (
                    f"Cross-Kante {edge.src} -> {edge.dst} entfernt",
                    _without_edge(pipeline, edge.src, edge.dst),
                    False,
                )
            )

    for wish in requested:
        if isinstance(wish, WhatIfTask):
            task = resolve_task_name(pipeline, wish.task)
            scenarios.append(
                (
                    f"Task {task} = {_num(wish.seconds)} s (angefragt)",
                    _with_duration(pipeline, task, wish.seconds),
                    True,
                )
            )
        else:
            scenarios.append(
                (
                    f"Cross-Kante {wish.src} -> {wish.dst} entfernt (angefragt)",
                    _without_edge(pipeline, wish.src, wish.dst),
                    True,
                )
            )

    rows = []
    for label, variant, angefragt in scenarios:
        lam = _lam_of(variant)
        rows.append(
            {
                "szenario": label,
                "lambda_s": lam,
                "delta_s": None if lam is None or base is None else lam - base,
                "angefragt": angefragt,
            }
        )
    rows.sort(key=lambda r: (r["lambda_s"] is not None, r["lambda_s"] or 0.0))
    return rows


def _provenance(dags: Sequence[ParsedDag]) -> dict[tuple[str, int], tuple[str, str, int]]:
    """Namespaced Kanten-Quelle plus Versatz -> (Signal, Datei, Zeile) der erzeugenden Kante.

    Mehrere Cross-Kanten mit derselben Quelle und demselben Versatz sind moeglich;
    dann steht hier die erste. Beide waeren echte, belegbare Kanten aus demselben DAG.
    """
    found: dict[tuple[str, int], tuple[str, str, int]] = {}
    for dag in dags:
        for edge in dag.cross:
            src = edge.src if edge.signal == "external_task_sensor" else node_name(dag, edge.src)
            found.setdefault((src, edge.periods), (edge.signal, edge.file, edge.lineno))
    return found


def _urteil(lam: float | None, takt_s: float | None) -> dict[str, Any]:
    if lam is None:
        return {"urteil": "nicht_anwendbar"}
    if takt_s is None:
        return {"urteil": "takt_unbekannt"}
    if abs(lam - takt_s) < GRENZBAND * takt_s:
        return {"urteil": "an_der_grenze"}
    if lam < takt_s:
        return {"urteil": "stabil", "reserve_prozent": (takt_s - lam) / takt_s * 100.0}
    drift = lam - takt_s
    return {
        "urteil": "instabil",
        "drift_s_pro_lauf": drift,
        "laeufe_bis_1h_rueckstand": 3600.0 / drift,
    }


def compose(
    *,
    pfad: str,
    dags: Sequence[ParsedDag],
    analysis: Analysis,
    stats: Mapping[str, TaskStats],
    statistic: Statistic,
    takt_s: float | None,
    takt_quelle: str | None,
    dauern_quelle: str,
    monte_carlo: MonteCarloResult | None,
    requested: Sequence[WhatIfTask | WhatIfDropEdge] = (),
) -> dict[str, Any]:
    pipeline = analysis.pipeline
    ns = [stats[task].n if task in stats else 0 for task in pipeline.tasks]

    kreis: dict[str, Any] | None = None
    if analysis.cycle is not None:
        herkunft = _provenance(dags)
        _, paths = condense(pipeline)
        kanten = []
        for edge in analysis.cycle:
            signal, datei, zeile = herkunft.get((edge.src, edge.periods), (None, None, None))
            kanten.append(
                {
                    "src": edge.src,
                    "dst": edge.dst,
                    "gewicht_s": edge.weight,
                    "perioden": edge.periods,
                    "signal": signal,
                    "datei": datei,
                    "zeile": zeile,
                    "task_pfad": list(paths[(edge.src, edge.dst, edge.periods)]),
                }
            )
        kreis = {"kondensiert": kanten, "aufgeloest": list(analysis.cycle_tasks)}

    mc: dict[str, Any] | None = None
    if monte_carlo is not None:
        mc = {
            "lambda_p50_s": monte_carlo.lam_p50,
            "lambda_p95_s": monte_carlo.lam_p95,
            "anteil_ueber_takt": monte_carlo.share_above_period,
            "samples": monte_carlo.samples,
            "seed": monte_carlo.seed,
            "konstant_gesampelt": list(monte_carlo.deterministic_tasks),
        }

    warnungen: list[dict[str, Any]] = [
        {"art": w.kind, "task": w.task or None, "datei": None, "zeile": None, "detail": w.detail}
        for w in analysis.warnings
    ]
    warnungen += [
        {"art": w.kind, "task": None, "datei": w.file, "zeile": w.lineno, "detail": w.detail}
        for w in analysis.parse_warnings
    ]

    return {
        "version": 1,
        "pfad": pfad,
        "dags": [
            {
                "dag_id": dag.dag_id,
                "datei": dag.file,
                "zeile": dag.lineno,
                "schedule": dag.schedule_expr,
                "takt_s": dag.period_s,
            }
            for dag in dags
        ],
        "takt_s": takt_s,
        "takt_quelle": takt_quelle,
        "dauern_quelle": dauern_quelle,
        "statistik": statistic,
        "stichprobe_laeufe_min": min(ns) if ns else None,
        "stichprobe_laeufe_median": float(statistics.median(ns)) if ns else None,
        "anwendbar": analysis.lam is not None,
        "lambda_s": analysis.lam,
        "critical_path_s": analysis.critical_path_s,
        "critical_path_tasks": list(analysis.critical_path_tasks),
        "reserve_prozent": None,
        "drift_s_pro_lauf": None,
        "laeufe_bis_1h_rueckstand": None,
        **_urteil(analysis.lam, takt_s),
        "kritischer_kreis": kreis,
        "monte_carlo": mc,
        "what_if": _what_if(analysis, requested),
        "warnungen": warnungen,
        "modellgrenzen": MODELLGRENZEN,
    }


# --- Text-Ausgabe ----------------------------------------------------------------------


def _num(x: float) -> str:
    text = f"{x:.2f}".rstrip("0").rstrip(".")
    return text.replace(".", ",")


def _dauer(seconds: float) -> str:
    base = f"{_num(seconds)} s"
    if seconds >= 5400:
        return f"{base} ({_num(seconds / 3600)} h)"
    if seconds >= 120:
        return f"{base} ({_num(seconds / 60)} min)"
    return base


STATISTIK_SATZ = {
    "mean": (
        "mean. Fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige"
        " Groesse; er ist ausreisserempfindlich, ein einzelner haengender Lauf kann ihn"
        " deutlich verschieben."
    ),
    "p50": "p50. Der Median ist robust gegen Ausreisser, unterschaetzt aber den Drift,"
    " wenn die Dauern rechtsschief streuen.",
    "p95": "p95. Bewusst pessimistisch: Lambda einer durchgehend schlechten Woche.",
}


def _kopf(d: dict[str, Any]) -> list[str]:
    zeilen = ["eigenlag analyze", "=" * 16, ""]
    for dag in d["dags"]:
        name = dag["dag_id"] if dag["dag_id"] is not None else "(dag_id nicht statisch)"
        schedule = f", Schedule {dag['schedule']}" if dag["schedule"] else ""
        zeilen.append(f"DAG:        {name} ({dag['datei']}:{dag['zeile']}{schedule})")
    if d["takt_s"] is not None:
        zeilen.append(f"Takt T:     {_dauer(d['takt_s'])}, Quelle: {d['takt_quelle']}")
    else:
        zeilen.append("Takt T:     unbekannt (kein statischer Schedule; --period setzt ihn)")
    zeilen.append(f"Dauern:     {d['dauern_quelle']}")
    zeilen.append(f"Statistik:  {STATISTIK_SATZ[d['statistik']]}")
    if d["stichprobe_laeufe_min"] is not None:
        zeilen.append(
            "Stichprobe: Laeufe je Task minimal"
            f" {d['stichprobe_laeufe_min']}, im Median {_num(d['stichprobe_laeufe_median'])}."
        )
    return zeilen


def _urteil_text(d: dict[str, Any]) -> list[str]:
    zeilen = ["", "Urteil", "-" * 6]
    lam, takt = d["lambda_s"], d["takt_s"]
    if d["urteil"] == "nicht_anwendbar":
        zeilen.append(
            "Nicht anwendbar: keine Cross-Run-Kante. Kein Lauf dieses DAGs wartet auf"
            " einen frueheren Lauf, es gibt keinen Kreis ueber die Zeitachse und damit"
            " keine strukturelle Taktgrenze. Der Takt wird allein von Kapazitaet und"
            " Laufzeit begrenzt, nicht von der Abhaengigkeitsstruktur."
        )
        return zeilen
    if d["urteil"] == "takt_unbekannt":
        zeilen.append(
            f"Lambda = {_dauer(lam)}: schneller kann diese Pipeline dauerhaft nicht"
            " takten. Der Takt T ist nicht bekannt (Schedule nicht statisch aufloesbar"
            " oder dataset-getriggert), deshalb gibt es kein Urteil stabil oder"
            " instabil. Mit --period SEKUNDEN wird der Vergleich gerechnet."
        )
        return zeilen
    if d["urteil"] == "an_der_grenze":
        zeilen.append(
            f"An der Grenze: Lambda = {_dauer(lam)} liegt innerhalb von 10 Prozent am"
            f" Takt T = {_dauer(takt)}. Vorsicht bei der Deutung: Systeme, deren Tasks"
            " auf Daten der laufenden Periode warten, pendeln sich genau hier ein, und"
            " die gemessenen Dauern sind dann bereits das Ergebnis dieses"
            " eingeschwungenen Zustands. Ob die Pipeline stabil ist oder driftet,"
            " entscheidet an dieser Grenze die Rueckkopplung, nicht die Messung."
        )
        return zeilen
    if d["urteil"] == "stabil":
        zeilen.append(
            f"Stabil: Lambda = {_dauer(lam)} liegt unter dem Takt T = {_dauer(takt)}."
            f" Reserve: {_num(d['reserve_prozent'])} %. Verspaetungen aus einem"
            " einzelnen Lauf klingen ab, statt sich aufzubauen."
        )
        return zeilen
    drift = d["drift_s_pro_lauf"]
    laeufe = d["laeufe_bis_1h_rueckstand"]
    zeilen.append(
        f"Instabil: Lambda = {_dauer(lam)} liegt ueber dem Takt T = {_dauer(takt)}."
        f" Die Verspaetung waechst um {_dauer(drift)} pro Lauf, unbegrenzt und"
        " unabhaengig von der Worker-Anzahl. Eine Stunde Rueckstand ist nach"
        f" {_num(laeufe)} Laeufen erreicht (etwa {_dauer(laeufe * lam)} Wanduhr-Zeit)."
        " Mehr Rechenleistung aendert daran nichts, weil der Engpass die"
        " Abhaengigkeitsstruktur ist, nicht die Kapazitaet."
    )
    return zeilen


def _kreis_text(d: dict[str, Any]) -> list[str]:
    kreis = d["kritischer_kreis"]
    if kreis is None:
        return []
    zeilen = ["", "Kritischer Kreis", "-" * 16]
    zeilen.append("Kondensiert (der Kreis in der Cross-Run-Matrix, sein Zyklusmittel ist Lambda):")
    for kante in kreis["kondensiert"]:
        beleg = ""
        if kante["datei"] is not None:
            beleg = f" [{kante['signal']}, {kante['datei']}:{kante['zeile']}]"
        perioden = (
            "1 Periode zurueck"
            if kante["perioden"] == 1
            else f"{kante['perioden']} Perioden zurueck"
        )
        zeilen.append(
            f"  {kante['src']} -> {kante['dst']}: Gewicht {_dauer(kante['gewicht_s'])},"
            f" {perioden}{beleg}"
        )
        if len(kante["task_pfad"]) > 1:
            zeilen.append(f"    als Task-Pfad: {' -> '.join(kante['task_pfad'])}")
    zeilen.append(f"Aufgeloest ueber alle Segmente: {' -> '.join(kreis['aufgeloest'])}")
    zeilen.append(
        "Der Weg zu einem kleineren Lambda fuehrt ueber diesen Kreis; eine Verkuerzung"
        " daneben aendert Lambda um exakt null. Ob eine einzelne Verkuerzung"
        " durchschlaegt oder ein zweiter Kreis mit gleichem Zyklusmittel uebernimmt,"
        " rechnet das What-if-Ranking unten nach."
    )
    return zeilen


def _mc_text(d: dict[str, Any]) -> list[str]:
    zeilen = ["", "Monte Carlo", "-" * 11]
    mc = d["monte_carlo"]
    if mc is None:
        zeilen.append(
            "Nicht gerechnet (abgeschaltet oder kein Kreis). Die Lambda-Angabe oben ist"
            " ein Punktwert auf der gewaehlten Statistik."
        )
        return zeilen
    zeilen.append(
        f"Lambda p50 = {_dauer(mc['lambda_p50_s'])}, Lambda p95 = {_dauer(mc['lambda_p95_s'])}"
        f" ({mc['samples']} Stichproben, Lognormal-Fit aus p50/p95 je Task, Seed"
        f" {mc['seed']}: derselbe Aufruf liefert dieselben Zahlen)."
    )
    zeilen.append(
        "p95 beantwortet, ob der Takt auch in einer schlechten Woche haelt, nicht nur"
        " im Durchschnitt."
    )
    takt = d["takt_s"]
    if takt is not None:
        if mc["lambda_p50_s"] < takt < mc["lambda_p95_s"]:
            zeilen.append(
                "Instabil in schlechten Wochen, erholt sich in guten: die Verspaetung"
                " pendelt statt zu wachsen. Sichtbar wird das als Pipeline, die"
                " gelegentlich hinterherlaeuft und sich scheinbar grundlos wieder faengt."
            )
        if mc["anteil_ueber_takt"] is not None:
            zeilen.append(
                f"Anteil der Stichproben mit Lambda ueber dem Takt:"
                f" {_num(mc['anteil_ueber_takt'] * 100)} %."
            )
    if mc["konstant_gesampelt"]:
        zeilen.append(
            "Konstant gesampelt (keine belastbare Streuung, angenommene oder duenne"
            f" Dauern): {', '.join(mc['konstant_gesampelt'])}. Die p95-Aussage"
            " unterschaetzt die Streuung dieser Tasks."
        )
    return zeilen


def _what_if_text(d: dict[str, Any]) -> list[str]:
    if not d["what_if"]:
        return []
    zeilen = ["", "What-if", "-" * 7]
    if d["lambda_s"] is not None:
        zeilen.append(f"Basis: Lambda = {_dauer(d['lambda_s'])}. Sortiert nach neuem Lambda.")
    for i, zeile in enumerate(d["what_if"], start=1):
        if zeile["lambda_s"] is None:
            wirkung = "kein Kreis mehr, Taktgrenze nicht anwendbar"
        else:
            wirkung = f"Lambda {_dauer(zeile['lambda_s'])}"
            if zeile["delta_s"] is not None:
                vorzeichen = "-" if zeile["delta_s"] < 0 else "+"
                wirkung += f", Veraenderung {vorzeichen}{_num(abs(zeile['delta_s']))} s"
        zeilen.append(f"  {i}. {zeile['szenario']}: {wirkung}")
    zeilen.append(
        "Eine Optimierung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um"
        " exakt null. Das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten;"
        " alles andere ist fuer die Taktgrenze wirkungslos, so nuetzlich es fuer die"
        " Latenz eines Einzellaufs sein mag."
    )
    return zeilen


WARN_TITEL = {
    "sensor_im_kritischen_kreis": "Sensor auf dem kritischen Kreis",
    "dauer_angenommen": "Dauer angenommen",
    "stichprobe_zu_klein": "Stichprobe zu klein",
    "sensor_not_modeled": "Sensor-Kante nicht modelliert",
    "sensor_dynamic_offset": "Sensor-Versatz nicht statisch bestimmbar",
    "include_prior_dates": "include_prior_dates nicht modelliert",
    "prev_run_success": "prev_*_success-Zugriff (keine Lambda-Kante)",
    "prev_run_date": "prev_ds-Zugriff (schwaches Signal)",
}


def _warn_text(d: dict[str, Any]) -> list[str]:
    zeilen = ["", "Warnungen", "-" * 9]
    if not d["warnungen"]:
        zeilen.append("Keine.")
    sensor_kreis = False
    f_befund = False
    for w in d["warnungen"]:
        titel = WARN_TITEL.get(w["art"], w["art"])
        wo = w["task"] if w["task"] else ""
        if w["datei"] is not None:
            wo = f"{w['datei']}:{w['zeile']}"
        detail = f" ({w['detail']})" if w["detail"] else ""
        zeilen.append(f"  - {titel}: {wo}{detail}")
        sensor_kreis = sensor_kreis or w["art"] == "sensor_im_kritischen_kreis"
        f_befund = f_befund or w["art"] == "prev_run_success"
    if sensor_kreis:
        zeilen.append(f"  {SENSOR_KREIS_TEXT}")
    if f_befund:
        zeilen.append(f"  {F_DIVERGENZ_TEXT}")
    return zeilen


def render(d: dict[str, Any]) -> str:
    zeilen = (
        _kopf(d)
        + _urteil_text(d)
        + _kreis_text(d)
        + _mc_text(d)
        + _what_if_text(d)
        + _warn_text(d)
        + ["", "Modellgrenzen", "-" * 13]
        + [f"  - {grenze}" for grenze in d["modellgrenzen"]]
    )
    zeilen.append("")
    return "\n".join(zeilen)
