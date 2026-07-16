"""Beschleunigungsplan (Spec 012, ADR-024): aus der Diagnose wird das Produkt.

Bisher sagt der Report "hier ist deine Grenze und wer schuld ist". Der Plan sagt:
"deine Pipeline koennte alle X laufen statt alle Y; hier ist die Aenderung, die den
Unterschied kauft, und was sie bringt". Jeder Befund wird als unbeanspruchte Reserve
formuliert, nicht als Mangel.

Zwei Gewinn-Formulierungen, exakt definiert (Vorentscheidung 4):
  instabil (lambda > T): eine Aktion macht den Takt tragfaehig genau dann, wenn
    lambda_neu < T; beziffert wird die weggeraeumte Drift (lambda - T pro Lauf).
    Rettet keine Einzel-Aktion, rechnet der Plan die Paare der Top-3 durch.
  stabil (lambda < T): der Gewinn ist Headroom; lambda ist die Untergrenze, ein Takt
    von lambda statt T liefe (24h/lambda - 24h/T) mal oefter und hielte die Daten bis
    zu (T - lambda) frischer. Das "bis zu" ist Pflicht (Regel 2: keine erfundene Marge).

Der Behebungs-Katalog (messages.py, plan_fix_*) ist Muster-Wissen je Kanten-Art, nie
eine Garantie: "ueblicher Weg", nicht "sicher". Die Kanten-Art eines cross_entfernt
kommt aus der Signal-Herkunft der geparsten DAGs; ein direkt gebautes Pipeline-Objekt
(die Demo) traegt kein Signal und bekommt keinen Katalog-Text.
"""

from __future__ import annotations

from collections.abc import Sequence
from itertools import combinations
from typing import Any

from eigenlag.analyze import Analysis
from eigenlag.maxplus import TOL, condense, howard
from eigenlag.model import Pipeline
from eigenlag.parse_airflow import ParsedDag, node_name

TAG_S = 86400.0
GRENZBAND = 0.10  # muss mit report.GRENZBAND uebereinstimmen

# Kanten-Art je Signal (wiki/signals.md A-G, plus dbt E). A/B/C/G stehen heute als
# Cross-Run-Kante im Graphen; D und E sind Muster-Wissen fuer spaeter, der Katalog
# ist damit vollstaendig, auch wo der Parser die Kante noch nicht erzeugt.
KANTEN_ART: dict[str, str] = {
    "depends_on_past": "A",
    "wait_for_downstream": "B",
    "external_task_sensor": "C",
    "include_prior_dates": "D",
    "max_active_runs": "G",
    "is_incremental": "E",
}
TASK_FIX_KEY = "plan_fix_task_halved"


def fix_key(signal: str) -> str:
    return f"plan_fix_{signal}"


def _signal_map(dags: Sequence[ParsedDag]) -> dict[tuple[str, str], str]:
    """(namespaced src, namespaced dst) -> Signal-Art, wie to_pipeline die Kante baut."""
    found: dict[tuple[str, str], str] = {}
    for dag in dags:
        for edge in dag.cross:
            src = edge.src if edge.signal == "external_task_sensor" else node_name(dag, edge.src)
            found.setdefault((src, node_name(dag, edge.dst)), edge.signal)
    return found


def _lam_of(pipeline: Pipeline) -> float | None:
    outcome = howard(condense(pipeline)[0])
    return None if outcome is None else outcome[0]


def _with_duration(pipeline: Pipeline, task: str, seconds: float) -> Pipeline:
    durations = dict(pipeline.durations)
    durations[task] = seconds
    return Pipeline(durations=durations, intra=pipeline.intra, cross=pipeline.cross)


def _without_edge(pipeline: Pipeline, src: str, dst: str) -> Pipeline:
    remaining = [e for e in pipeline.cross if not (e.src == src and e.dst == dst)]
    return Pipeline(durations=pipeline.durations, intra=pipeline.intra, cross=remaining)


def _apply(pipeline: Pipeline, row: dict[str, Any]) -> Pipeline:
    if row["art"] in ("task_halbiert", "task_gesetzt"):
        return _with_duration(pipeline, row["task"], float(row["wert_s"]))
    return _without_edge(pipeline, row["src"], row["dst"])


def _urteil(base: float | None, takt_s: float | None) -> str:
    if base is None:
        return "nicht_anwendbar"
    if takt_s is None:
        return "takt_unbekannt"
    if abs(base - takt_s) < GRENZBAND * takt_s:
        return "an_der_grenze"
    return "stabil" if base < takt_s else "instabil"


def _headroom(lam: float, takt_s: float) -> dict[str, float]:
    return {
        "laeufe_pro_tag_lambda": TAG_S / lam,
        "laeufe_pro_tag_takt": TAG_S / takt_s,
        "laeufe_pro_tag_mehr": TAG_S / lam - TAG_S / takt_s,
        "frische_delta_s": takt_s - lam,
    }


def _gewinn(
    urteil: str, lam: float | None, base: float, takt_s: float | None, tragfaehig: bool
) -> dict[str, float]:
    if urteil == "instabil" and takt_s is not None:
        return {"weggeraeumte_drift_s": base - takt_s} if tragfaehig else {}
    if urteil == "stabil" and takt_s is not None and lam is not None and lam < base:
        return _headroom(lam, takt_s)
    return {}


def _enrich(
    row: dict[str, Any],
    base: float,
    takt_s: float | None,
    urteil: str,
    signal_map: dict[tuple[str, str], str],
) -> dict[str, Any]:
    signal = signal_map.get((row["src"], row["dst"])) if row["art"] == "cross_entfernt" else None
    if row["art"] == "cross_entfernt":
        katalog = fix_key(signal) if signal in KANTEN_ART else None
    elif row["art"] == "task_halbiert":
        katalog = TASK_FIX_KEY
    else:
        katalog = None
    lam = row["lambda_s"]
    delta = row["delta_s"]
    tragfaehig = lam is None or (takt_s is not None and lam < takt_s)
    return {
        **row,
        "signal": signal,
        "kanten_art": KANTEN_ART.get(signal) if signal is not None else None,
        "katalog_schluessel": katalog,
        "lambda_neu_s": lam,
        "delta_prozent": (delta / base * 100.0) if (delta is not None and base) else None,
        "macht_tragfaehig": tragfaehig,
        "gewinn": _gewinn(urteil, lam, base, takt_s, tragfaehig),
    }


def _effektivitaet(action: dict[str, Any]) -> float:
    """Sortierschluessel: kein Kreis mehr (None) ist am wirksamsten, sonst kleines lambda."""
    lam = action["lambda_neu_s"]
    return float("-inf") if lam is None else lam


def _paar_rechnung(
    pipeline: Pipeline, actions: Sequence[dict[str, Any]], takt_s: float | None
) -> dict[str, Any] | None:
    """Die zwei guenstigsten Aktionen zusammen: nur die drei wirksamsten Einzel-Aktionen
    werden gepaart (drei Paare), damit die Kombinatorik nicht explodiert."""
    top = sorted(actions, key=_effektivitaet)[:3]
    if len(top) < 2:
        return None
    bestes: dict[str, Any] | None = None
    for a, b in combinations(top, 2):
        lam = _lam_of(_apply(_apply(pipeline, a), b))
        if bestes is None or _effektivitaet({"lambda_neu_s": lam}) < _effektivitaet(bestes):
            bestes = {"a": a, "b": b, "lambda_neu_s": lam}
    assert bestes is not None
    lam = bestes["lambda_neu_s"]
    bestes["macht_tragfaehig"] = lam is None or (takt_s is not None and lam < takt_s)
    return bestes


def build_plan(
    *,
    rows: Sequence[dict[str, Any]],
    analysis: Analysis,
    dags: Sequence[ParsedDag],
    takt_s: float | None,
) -> dict[str, Any]:
    """Baut den Plan aus den What-if-Zeilen (Vorentscheidung 3: dieselbe Rechnung),
    angereichert um Kanten-Art, Katalog-Schluessel, Delta in Prozent und die
    verdict-abhaengigen Gewinn-Felder. Sprachneutral; render_plan waehlt die Sprache."""
    base = analysis.lam
    urteil = _urteil(base, takt_s)
    signal_map = _signal_map(dags)

    if base is None:
        return {
            "urteil": urteil,
            "basis_lambda_s": None,
            "takt_s": takt_s,
            "aktionen": [],
            "null_delta": [],
            "headroom": None,
            "kein_einzel_ausreichend": False,
            "paar_rechnung": None,
        }

    actions = [_enrich(row, base, takt_s, urteil, signal_map) for row in rows]
    gezeigt = [
        a for a in actions if a["angefragt"] or a["delta_s"] is None or abs(a["delta_s"]) > TOL
    ]
    null_delta = [a for a in actions if a not in gezeigt]

    headroom = _headroom(base, takt_s) if (urteil == "stabil" and takt_s is not None) else None

    kein_einzel = urteil == "instabil" and not any(a["macht_tragfaehig"] for a in actions)
    paar = _paar_rechnung(analysis.pipeline, actions, takt_s) if kein_einzel else None

    return {
        "urteil": urteil,
        "basis_lambda_s": base,
        "takt_s": takt_s,
        "aktionen": gezeigt,
        "null_delta": null_delta,
        "headroom": headroom,
        "kein_einzel_ausreichend": kein_einzel,
        "paar_rechnung": paar,
    }
