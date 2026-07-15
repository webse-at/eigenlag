"""Tests-zuerst fuer den Report (Spec 009, Auftrag 2).

Der Report ist das Produkt: er wird von Leuten gelesen, die kein Wiki kennen. Die
Tests pinnen deshalb nicht nur Zahlen, sondern Formulierungen, die die Spec vorgibt:
"nicht anwendbar" statt Lambda = 0 (ADR-007), der Kreis doppelt (ADR-002), der
Pendel-Satz bei p95 > T > p50, der Exakt-null-Satz des What-if-Rankings und der
nie abschaltbare Warnblock samt Modellgrenzen-Fusszeile.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pytest

from eigenlag.analyze import analyze_result
from eigenlag.durations import Statistic, TaskStats, assume
from eigenlag.montecarlo import MonteCarloResult
from eigenlag.parse_airflow import parse_source
from eigenlag.report import WhatIfDropEdge, WhatIfTask, compose, render

QUELLE = """\
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="takt", schedule="@hourly") as dag:
    lade = BashOperator(task_id="lade", depends_on_past=True, bash_command="x")
    rechne = EmptyOperator(task_id="rechne")
    lade >> rechne
"""

OHNE_KREIS = """\
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="takt", schedule="@hourly") as dag:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    a >> b
"""


def stats_mit(lade_mean: float) -> dict[str, TaskStats]:
    return {
        "takt.lade": TaskStats(
            p50=lade_mean, p95=lade_mean * 1.2, mean=lade_mean, n=12, operator="BashOperator"
        ),
        "takt.rechne": TaskStats(p50=600.0, p95=700.0, mean=600.0, n=40, operator="EmptyOperator"),
    }


def bericht(
    source: str = QUELLE,
    stats: dict[str, TaskStats] | None = None,
    takt_s: float | None = 3600.0,
    takt_quelle: str | None = 'Schedule "@hourly"',
    monte_carlo: MonteCarloResult | None = None,
    requested: Sequence[WhatIfTask | WhatIfDropEdge] = (),
    fallback: TaskStats | None = None,
    statistic: Statistic = "mean",
) -> dict[str, Any]:
    result = parse_source(source, "dags/takt.py")
    if stats is None:
        stats = stats_mit(1800.0)
    analysis = analyze_result(result, stats, statistic, fallback)
    return compose(
        pfad="dags/takt.py",
        dags=result.dags,
        analysis=analysis,
        stats=stats,
        statistic=statistic,
        takt_s=takt_s,
        takt_quelle=takt_quelle,
        dauern_quelle="Test-Fixture",
        monte_carlo=monte_carlo,
        requested=requested,
    )


# --- Das Urteil ------------------------------------------------------------------------


def test_stabil_mit_reserve_in_prozent() -> None:
    d = bericht(stats=stats_mit(1800.0))
    assert d["urteil"] == "stabil"
    assert d["lambda_s"] == 1800.0
    assert d["reserve_prozent"] == pytest.approx(50.0)
    text = render(d)
    assert "stabil" in text.lower()
    assert "Reserve: 50 %" in text


def test_an_der_grenze_binnen_10_prozent_mit_rueckkopplungs_hinweis() -> None:
    d = bericht(stats=stats_mit(3500.0))
    assert d["urteil"] == "an_der_grenze"
    text = render(d)
    assert "an der grenze" in text.lower()
    assert "eingeschwungen" in text


def test_instabil_mit_drift_und_zeit_bis_eine_stunde_rueckstand() -> None:
    d = bericht(stats=stats_mit(4500.0))
    assert d["urteil"] == "instabil"
    assert d["drift_s_pro_lauf"] == pytest.approx(900.0)
    assert d["laeufe_bis_1h_rueckstand"] == pytest.approx(4.0)
    text = render(d)
    assert "pro Lauf" in text
    assert "Stunde" in text


def test_kein_kreis_ist_nicht_anwendbar_und_ausdruecklich_kein_lambda_null() -> None:
    stats = {
        "takt.a": TaskStats(p50=60.0, p95=70.0, mean=60.0, n=12, operator=None),
        "takt.b": TaskStats(p50=30.0, p95=40.0, mean=30.0, n=12, operator=None),
    }
    d = bericht(source=OHNE_KREIS, stats=stats)
    assert d["urteil"] == "nicht_anwendbar"
    assert d["anwendbar"] is False
    assert d["lambda_s"] is None
    text = render(d)
    assert "nicht anwendbar" in text.lower()
    assert "keine Cross-Run-Kante" in text
    assert "Lambda = 0" not in text and "= 0 s" not in text


def test_takt_unbekannt_verlangt_period_statt_zu_raten() -> None:
    d = bericht(takt_s=None, takt_quelle=None)
    assert d["urteil"] == "takt_unbekannt"
    assert "--period" in render(d)


# --- Der kritische Kreis, doppelt (ADR-002) ---------------------------------------------


def test_kreis_kondensiert_und_aufgeloest_mit_herkunft() -> None:
    d = bericht()
    kreis = d["kritischer_kreis"]
    assert kreis is not None
    kanten = kreis["kondensiert"]
    assert [(k["src"], k["dst"], k["perioden"]) for k in kanten] == [("takt.lade", "takt.lade", 1)]
    assert kanten[0]["signal"] == "depends_on_past"
    assert kanten[0]["datei"] == "dags/takt.py"
    assert kanten[0]["zeile"] > 0
    assert kreis["aufgeloest"] == ["takt.lade"]
    text = render(d)
    assert "dags/takt.py:" in text


# --- Monte Carlo -----------------------------------------------------------------------


def test_monte_carlo_pendel_satz_wenn_p95_ueber_takt_und_p50_darunter() -> None:
    mc = MonteCarloResult(
        lam_p50=3000.0,
        lam_p95=4800.0,
        share_above_period=0.2,
        samples=1000,
        seed=1,
        deterministic_tasks=(),
    )
    d = bericht(monte_carlo=mc)
    assert d["monte_carlo"]["lambda_p50_s"] == 3000.0
    assert d["monte_carlo"]["lambda_p95_s"] == 4800.0
    text = render(d)
    assert "pendelt" in text
    assert "schlechten Wochen" in text


def test_monte_carlo_abgeschaltet_wird_benannt() -> None:
    text = render(bericht(monte_carlo=None))
    assert "Monte Carlo" in text


# --- What-if ----------------------------------------------------------------------------


def test_standard_ranking_halbiert_kreis_tasks_und_entfernt_cross_kanten() -> None:
    d = bericht(requested=[WhatIfTask(task="takt.rechne", seconds=10.0)])
    szenarien = d["what_if"]
    labels = [s["szenario"] for s in szenarien]
    assert any("takt.lade" in label and "halbiert" in label for label in labels)
    assert any("entfernt" in label for label in labels)
    # Kante entfernt: der Kreis ist weg, Lambda nicht anwendbar — sortiert nach vorn.
    assert szenarien[0]["lambda_s"] is None
    halbiert = next(s for s in szenarien if "halbiert" in s["szenario"])
    assert halbiert["lambda_s"] == pytest.approx(900.0)
    assert halbiert["delta_s"] == pytest.approx(-900.0)
    # Die angefragte Nicht-Kreis-Optimierung bringt exakt null.
    angefragt = next(s for s in szenarien if s["angefragt"])
    assert angefragt["delta_s"] == pytest.approx(0.0)
    text = render(d)
    assert "exakt null" in text


def test_requested_drop_edge_wird_gerechnet() -> None:
    d = bericht(requested=[WhatIfDropEdge(src="takt.lade", dst="takt.lade")])
    angefragt = [s for s in d["what_if"] if s["angefragt"]]
    assert len(angefragt) == 1
    assert angefragt[0]["lambda_s"] is None


# --- Null-Delta-Kompaktierung (Abnahme 009a) ---------------------------------------------


def test_compose_markiert_kreis_zugehoerigkeit_der_szenarien() -> None:
    d = bericht(requested=[WhatIfTask(task="takt.rechne", seconds=10.0)])
    halbiert = next(s for s in d["what_if"] if "halbiert" in s["szenario"])
    assert halbiert["auf_kreis"] is True
    entfernt = next(s for s in d["what_if"] if s["szenario"].startswith("Cross-Kante"))
    assert entfernt["auf_kreis"] is True
    angefragt = next(s for s in d["what_if"] if s["angefragt"])
    assert angefragt["auf_kreis"] is False


def zeile(
    szenario: str,
    lam: float | None,
    delta: float | None,
    angefragt: bool = False,
    auf_kreis: bool = False,
) -> dict[str, Any]:
    return {
        "szenario": szenario,
        "lambda_s": lam,
        "delta_s": delta,
        "angefragt": angefragt,
        "auf_kreis": auf_kreis,
    }


def test_null_delta_zeilen_werden_zur_sammelzeile_kompaktiert() -> None:
    d = bericht()
    d["what_if"] = [
        zeile("Task takt.lade halbiert (auf 900 s)", 900.0, -900.0, auf_kreis=True),
        zeile("Cross-Kante a -> a entfernt", 1800.0, 0.0, auf_kreis=True),
        zeile("Cross-Kante b -> b entfernt", 1800.0, 0.0),
        zeile("Cross-Kante c -> c entfernt", 1800.0, 0.0),
        zeile("Task takt.rechne = 10 s (angefragt)", 1800.0, 0.0, angefragt=True),
    ]
    text = render(d)
    assert "1. Task takt.lade halbiert" in text
    assert "2. Task takt.rechne = 10 s (angefragt)" in text
    assert (
        "3 weitere Szenarien aendern Lambda nicht: 1 Kreis-Gleichstand,"
        " 2 Kanten ausserhalb des kritischen Kreises." in text
    )
    assert "Cross-Kante a -> a entfernt" not in text


def test_sammelzeile_singular_und_nur_eine_kategorie() -> None:
    d = bericht()
    d["what_if"] = [
        zeile("Task takt.lade halbiert (auf 900 s)", 900.0, -900.0, auf_kreis=True),
        zeile("Cross-Kante a -> a entfernt", 1800.0, 0.0, auf_kreis=True),
    ]
    text = render(d)
    assert "1 weiteres Szenario aendert Lambda nicht: 1 Kreis-Gleichstand." in text


def test_lauter_null_deltas_ergeben_nur_die_sammelzeile() -> None:
    # Der 009a-Flaggschiff-Fall: uniforme Assume-Dauern, alle 15 Zeilen +0 s.
    d = bericht()
    d["what_if"] = [
        zeile("Task t1 halbiert (auf 150 s)", 600.0, 0.0, auf_kreis=True),
        zeile("Task t2 halbiert (auf 150 s)", 600.0, 0.0, auf_kreis=True),
        zeile("Cross-Kante x -> y entfernt", 600.0, 0.0, auf_kreis=True),
        zeile("Cross-Kante u -> v entfernt", 600.0, 0.0),
        zeile("Cross-Kante w -> w entfernt", 600.0, 0.0),
    ]
    text = render(d)
    assert (
        "5 weitere Szenarien aendern Lambda nicht: 3 Kreis-Gleichstaende,"
        " 2 Kanten ausserhalb des kritischen Kreises." in text
    )
    assert "  1. " not in text


def test_kein_kreis_mehr_zeile_wird_nie_kompaktiert() -> None:
    d = bericht()
    d["what_if"] = [
        zeile("Cross-Kante takt.lade -> takt.lade entfernt", None, None, auf_kreis=True),
        zeile("Cross-Kante b -> b entfernt", 1800.0, 0.0),
    ]
    text = render(d)
    assert "kein Kreis mehr" in text


def test_schlusssatz_beschreibt_das_verhalten() -> None:
    # 009a: der alte Satz behauptete "zeigt nur Kreis-Tasks und Cross-Kanten",
    # tatsaechlich rechnet das Ranking alle Cross-Kanten durch.
    text = render(bericht())
    assert "exakt null" in text
    assert "alle Cross-Kanten" in text
    assert "zeigt deshalb nur" not in text


# --- Pflicht-Warnblock und Modellgrenzen ------------------------------------------------


def test_angenommene_dauern_stehen_im_warnblock() -> None:
    stats = {"takt.lade": stats_mit(1800.0)["takt.lade"]}
    d = bericht(stats=stats, fallback=assume(300.0))
    arten = {w["art"] for w in d["warnungen"]}
    assert "dauer_angenommen" in arten
    text = render(d)
    assert "takt.rechne" in text
    assert "angenommen" in text


def test_sensor_im_kritischen_kreis_warnt_im_report() -> None:
    # Der Operator-Klassenname macht den Task zum Sensor; er liegt auf dem Kreis.
    stats = stats_mit(1800.0)
    stats["takt.lade"] = TaskStats(
        p50=1800.0, p95=2160.0, mean=1800.0, n=12, operator="ExternalTaskSensor"
    )
    d = bericht(stats=stats)
    arten = {w["art"] for w in d["warnungen"]}
    assert "sensor_im_kritischen_kreis" in arten
    text = render(d)
    assert "Wartezeit" in text


def test_modellgrenzen_fusszeile_steht_immer() -> None:
    text = render(bericht())
    assert "Untergrenze" in text
    assert "Retries" in text
    assert "Makespan" in text


def test_json_ist_serialisierbar_und_schema_stabil() -> None:
    d = bericht(
        monte_carlo=MonteCarloResult(
            lam_p50=1800.0,
            lam_p95=2200.0,
            share_above_period=0.0,
            samples=1000,
            seed=1,
            deterministic_tasks=("takt.rechne",),
        )
    )
    zurueck = json.loads(json.dumps(d, ensure_ascii=False))
    # Stabile Top-Level-Keys: ab Session 010 liest das CI-Gate genau diese Felder.
    assert set(zurueck) >= {
        "version",
        "pfad",
        "dags",
        "takt_s",
        "takt_quelle",
        "dauern_quelle",
        "statistik",
        "stichprobe_laeufe_min",
        "stichprobe_laeufe_median",
        "anwendbar",
        "lambda_s",
        "critical_path_s",
        "urteil",
        "kritischer_kreis",
        "monte_carlo",
        "what_if",
        "warnungen",
        "modellgrenzen",
    }
    assert zurueck["monte_carlo"]["konstant_gesampelt"] == ["takt.rechne"]
