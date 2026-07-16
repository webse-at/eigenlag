"""Tests-zuerst fuer den Beschleunigungsplan (Spec 012, ADR-024).

Aus der Diagnose wird das Produkt: der Plan formuliert jeden Befund als
unbeanspruchte Reserve, nicht als Mangel. Zwei Gewinn-Formulierungen, exakt
definiert (Vorentscheidung 4): instabil macht den Takt tragfaehig, stabil
beziffert Headroom. Der Behebungs-Katalog ist Muster-Wissen ("commonly resolved
by ..."), kein Orakel.

Die Demo-Pipeline (Prototyp, Ground Truth: lambda = 4.40, T = 3.0, instabil) ist
der Pin, weil sie die Verkaufsgeschichte IST: die Kante monitor -> core zu
entfernen macht den Takt tragfaehig (lambda_neu = 2.50 < 3.0), das retrain-Upgrade
(halbiert -> 3.60) rettet ihn nicht.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from eigenlag.analyze import Analysis, analyze_result
from eigenlag.demo import demo
from eigenlag.durations import TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.messages import CATALOG
from eigenlag.model import CrossEdge, Pipeline
from eigenlag.parse_airflow import parse_source
from eigenlag.plan import KANTEN_ART, TASK_FIX_KEY, build_plan, fix_key
from eigenlag.report import _what_if

# --- Katalog-Vollstaendigkeit (Akzeptanz: jede Kanten-Art x beide Sprachen) --------------


def test_jede_kanten_art_hat_ein_behebungs_muster_in_beiden_sprachen() -> None:
    for signal in KANTEN_ART:
        key = fix_key(signal)
        assert key in CATALOG["en"], f"EN fehlt {key}"
        assert key in CATALOG["de"], f"DE fehlt {key}"
    assert TASK_FIX_KEY in CATALOG["en"] and TASK_FIX_KEY in CATALOG["de"]


def test_kanten_arten_decken_a_bis_g_und_dbt_e_ab() -> None:
    assert set(KANTEN_ART.values()) == {"A", "B", "C", "D", "G", "E"}


# --- Plan auf der Demo-Pipeline (instabil, Prototyp-Ground-Truth) ------------------------


def demo_analysis(cross: list[Any] | None = None) -> Analysis:
    pipeline = demo(cross=cross)
    graph, paths = condense(pipeline)
    outcome = howard(graph)
    assert outcome is not None
    lam, cycle = outcome
    tasks: list[str] = []
    for edge in cycle:
        for task in paths[(edge.src, edge.dst, edge.periods)]:
            if task not in tasks:
                tasks.append(task)
    return Analysis(
        pipeline=pipeline,
        lam=lam,
        cycle=tuple(cycle),
        cycle_tasks=tuple(tasks),
        critical_path_s=0.0,
        critical_path_tasks=[],
        parse_warnings=(),
        warnings=(),
    )


def demo_plan(takt_s: float = 3.0) -> dict[str, Any]:
    analysis = demo_analysis()
    rows = _what_if(analysis, ())
    return build_plan(rows=rows, analysis=analysis, dags=(), takt_s=takt_s)


def aktion(plan: dict[str, Any], **match: Any) -> dict[str, Any]:
    for a in plan["aktionen"]:
        if all(a.get(k) == v for k, v in match.items()):
            return cast("dict[str, Any]", a)
    raise AssertionError(f"keine Aktion mit {match} in {[a['szenario'] for a in plan['aktionen']]}")


def test_demo_urteil_instabil_und_basis_lambda() -> None:
    plan = demo_plan()
    assert plan["urteil"] == "instabil"
    assert plan["basis_lambda_s"] == pytest.approx(4.40)
    assert plan["takt_s"] == pytest.approx(3.0)


def test_demo_kante_monitor_core_macht_takt_tragfaehig() -> None:
    plan = demo_plan()
    a = aktion(plan, art="cross_entfernt", src="monitor", dst="core")
    assert a["lambda_neu_s"] == pytest.approx(2.50)
    assert a["delta_s"] == pytest.approx(-1.90)
    assert a["delta_prozent"] == pytest.approx(-43.18, abs=0.01)
    assert a["macht_tragfaehig"] is True


def test_demo_retrain_halbiert_rettet_den_takt_nicht() -> None:
    plan = demo_plan()
    a = aktion(plan, art="task_halbiert", task="retrain")
    assert a["lambda_neu_s"] == pytest.approx(3.60)
    assert a["macht_tragfaehig"] is False


def test_demo_core_halbiert_rettet_den_takt_nicht() -> None:
    plan = demo_plan()
    a = aktion(plan, art="task_halbiert", task="core")
    assert a["lambda_neu_s"] == pytest.approx(3.85)
    assert a["macht_tragfaehig"] is False


def test_demo_genau_eine_einzel_aktion_ist_tragfaehig() -> None:
    plan = demo_plan()
    tragfaehig = [a for a in plan["aktionen"] if a["macht_tragfaehig"]]
    assert len(tragfaehig) == 1
    assert (tragfaehig[0]["src"], tragfaehig[0]["dst"]) == ("monitor", "core")
    assert plan["kein_einzel_ausreichend"] is False


def test_demo_weggeraeumte_drift_ist_lambda_minus_takt() -> None:
    plan = demo_plan()
    a = aktion(plan, art="cross_entfernt", src="monitor", dst="core")
    # Bei T = 3.0 driftet die Pipeline um lambda - T = 1.40 h/Lauf; die tragfaehige
    # Aktion raeumt genau diese Drift weg.
    assert a["gewinn"]["weggeraeumte_drift_s"] == pytest.approx(1.40)


def test_demo_null_delta_aktionen_wandern_in_null_delta() -> None:
    plan = demo_plan()
    # core -> core aendert lambda nicht (monitor-Loop bindet), gehoert in null_delta.
    szen = [(a["src"], a["dst"]) for a in plan["null_delta"] if a["art"] == "cross_entfernt"]
    assert ("core", "core") in szen
    # keine gezeigte Aktion hat Delta null.
    for a in plan["aktionen"]:
        assert a["lambda_neu_s"] is None or a["delta_s"] < 0


# --- Signal-Herkunft und Katalog aus einem geparsten DAG --------------------------------


DAG_INSTABIL = """\
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="takt", schedule="@hourly") as dag:
    lade = BashOperator(task_id="lade", depends_on_past=True, bash_command="x")
    rechne = EmptyOperator(task_id="rechne")
    lade >> rechne
"""


def parsed_plan(source: str, stats: dict[str, TaskStats], takt_s: float | None) -> dict[str, Any]:
    result = parse_source(source, "dags/takt.py")
    analysis = analyze_result(result, stats, "mean", None)
    rows = _what_if(analysis, ())
    return build_plan(rows=rows, analysis=analysis, dags=result.dags, takt_s=takt_s)


def test_cross_aktion_traegt_kanten_art_und_katalog_schluessel() -> None:
    stats = {
        "takt.lade": TaskStats(p50=4500.0, p95=5400.0, mean=4500.0, n=12, operator="BashOperator"),
        "takt.rechne": TaskStats(p50=600.0, p95=700.0, mean=600.0, n=40, operator=None),
    }
    plan = parsed_plan(DAG_INSTABIL, stats, takt_s=3600.0)
    a = aktion(plan, art="cross_entfernt", src="takt.lade", dst="takt.lade")
    assert a["signal"] == "depends_on_past"
    assert a["kanten_art"] == "A"
    assert a["katalog_schluessel"] == fix_key("depends_on_past")


def test_demo_ohne_dags_hat_keine_signal_herkunft() -> None:
    # Die Demo ist ein direkt gebautes Pipeline-Objekt ohne ParsedDag: kein Signal,
    # kein Katalog-Text (kein erfundenes Detailwissen ueber fremde Tasks).
    plan = demo_plan()
    a = aktion(plan, art="cross_entfernt", src="monitor", dst="core")
    assert a["signal"] is None
    assert a["kanten_art"] is None


# --- Stabiler Fall: Headroom (Laeufe pro Tag, Frische) ----------------------------------


def stabil_plan(lade_mean: float, takt_s: float) -> dict[str, Any]:
    stats = {
        "takt.lade": TaskStats(
            p50=lade_mean, p95=lade_mean * 1.2, mean=lade_mean, n=12, operator="BashOperator"
        ),
        "takt.rechne": TaskStats(p50=600.0, p95=700.0, mean=600.0, n=40, operator=None),
    }
    return parsed_plan(DAG_INSTABIL, stats, takt_s=takt_s)


def test_stabil_headroom_laeufe_pro_tag_und_frische() -> None:
    # lambda = 600 s, T = 3600 s (der Flaggschiff-Fall in klein): Reserve wird konkret.
    plan = stabil_plan(lade_mean=600.0, takt_s=3600.0)
    assert plan["urteil"] == "stabil"
    hr = plan["headroom"]
    assert hr["laeufe_pro_tag_lambda"] == pytest.approx(144.0)  # 86400 / 600
    assert hr["laeufe_pro_tag_takt"] == pytest.approx(24.0)  # 86400 / 3600
    assert hr["laeufe_pro_tag_mehr"] == pytest.approx(120.0)
    assert hr["frische_delta_s"] == pytest.approx(3000.0)  # T - lambda, "up to"


def test_instabil_hat_kein_headroom() -> None:
    plan = demo_plan()
    assert plan["headroom"] is None


# --- Instabil ohne rettende Einzel-Aktion: Paar-Rechnung der Top-3 -----------------------


def cobinding_analysis() -> Analysis:
    # Zwei gleich schwere, unabhaengige Selbst-Loops (a=3.0, b=3.0). lambda = 3.0.
    # Jede Einzel-Aktion laesst den jeweils anderen Loop bindend: kein einzelnes
    # Entfernen und kein einzelnes Halbieren bringt lambda unter T = 2.8. Erst beide
    # Kanten zusammen entfernt loesen den Kreis ganz auf.
    durations = {"a": 3.0, "b": 3.0, "c": 0.1}
    cross = [CrossEdge("a", "a", 1), CrossEdge("b", "b", 1)]
    pipeline = Pipeline(durations=durations, intra=[("a", "c"), ("b", "c")], cross=cross)
    graph, paths = condense(pipeline)
    outcome = howard(graph)
    assert outcome is not None
    lam, cycle = outcome
    tasks: list[str] = []
    for edge in cycle:
        for task in paths[(edge.src, edge.dst, edge.periods)]:
            if task not in tasks:
                tasks.append(task)
    return Analysis(
        pipeline=pipeline,
        lam=lam,
        cycle=tuple(cycle),
        cycle_tasks=tuple(tasks),
        critical_path_s=0.0,
        critical_path_tasks=[],
        parse_warnings=(),
        warnings=(),
    )


def test_paar_rechnung_wenn_keine_einzel_aktion_reicht() -> None:
    analysis = cobinding_analysis()
    rows = _what_if(analysis, ())
    plan = build_plan(rows=rows, analysis=analysis, dags=(), takt_s=2.5)
    assert plan["urteil"] == "instabil"
    assert plan["basis_lambda_s"] == pytest.approx(3.0)
    # Keine Einzel-Aktion bringt lambda unter 2.8 (der jeweils andere Loop bleibt bindend).
    assert not any(a["macht_tragfaehig"] for a in plan["aktionen"])
    assert plan["kein_einzel_ausreichend"] is True
    paar = plan["paar_rechnung"]
    assert paar is not None
    # Beide Selbst-Kanten zusammen entfernt: kein Kreis mehr, Taktgrenze aufgeloest.
    assert paar["lambda_neu_s"] is None
    assert paar["macht_tragfaehig"] is True
