"""Tests-zuerst fuer die Zweisprachigkeit end-to-end (Spec 011, ADR-023).

Drei Garantien: --json ist ueber beide Sprachen byte-identisch (die Sprache
beruehrt nur render, nie compose), der englische Report enthaelt keine deutschen
Formulierungen mehr, und die Grenzfaelle (kein Kreis) tragen in beiden Sprachen
den richtigen Satz statt eines stillen Fallbacks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from eigenlag.analyze import analyze_result
from eigenlag.durations import TaskStats
from eigenlag.parse_airflow import parse_source
from eigenlag.report import compose, render

DAG_STABIL = """\
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


def schreibe(tmp_path: Path, source: str = DAG_STABIL) -> str:
    ziel = tmp_path / "takt.py"
    ziel.write_text(source)
    return str(ziel)


def _bericht(
    source: str, stats: dict[str, TaskStats], takt_s: float | None = 3600.0
) -> dict[str, Any]:
    result = parse_source(source, "dags/takt.py")
    analysis = analyze_result(result, stats, "mean", None)
    return compose(
        pfad="dags/takt.py",
        dags=result.dags,
        analysis=analysis,
        stats=stats,
        statistic="mean",
        takt_s=takt_s,
        takt_quelle='Schedule "@hourly"',
        dauern_quelle="Test-Fixture",
        monte_carlo=None,
    )


STABIL_STATS = {
    "takt.lade": TaskStats(p50=1800.0, p95=2160.0, mean=1800.0, n=12, operator="BashOperator"),
    "takt.rechne": TaskStats(p50=600.0, p95=700.0, mean=600.0, n=40, operator="EmptyOperator"),
}
OHNE_KREIS_STATS = {
    "takt.a": TaskStats(p50=60.0, p95=70.0, mean=60.0, n=12, operator=None),
    "takt.b": TaskStats(p50=30.0, p95=40.0, mean=30.0, n=12, operator=None),
}


# --- --json ist sprachunabhaengig -------------------------------------------------------


def test_analyze_json_byte_identisch_ueber_sprachen(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from eigenlag.cli import main

    pfad = schreibe(tmp_path)
    main(["analyze", pfad, "--assume-duration", "600", "--json", "--lang", "en"])
    en = capsys.readouterr().out
    main(["analyze", pfad, "--assume-duration", "600", "--json", "--lang", "de"])
    de = capsys.readouterr().out
    assert en == de
    assert en != ""


def test_check_json_byte_identisch_ueber_sprachen(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import subprocess

    from eigenlag.cli import main

    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    (repo / "dags/pipeline.py").write_text(OHNE_KREIS)

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)

    git("init", "-q")
    git("-c", "user.name=t", "-c", "user.email=t@t.invalid", "commit", "--allow-empty", "-qm", "v1")
    git("tag", "v1")
    (repo / "dags/pipeline.py").write_text(DAG_STABIL)

    main(["check", str(repo / "dags"), "--against", "v1", "--json", "--lang", "en"])
    en = capsys.readouterr().out
    main(["check", str(repo / "dags"), "--against", "v1", "--json", "--lang", "de"])
    de = capsys.readouterr().out
    assert en == de
    assert en != ""


# --- Kein-Kreis in beiden Sprachen ------------------------------------------------------


def test_kein_kreis_deutsch_und_englisch() -> None:
    d = _bericht(OHNE_KREIS, OHNE_KREIS_STATS)
    assert d["urteil"] == "nicht_anwendbar"

    de = render(d, "de")
    assert "nicht anwendbar" in de.lower()
    assert "keine Cross-Run-Kante" in de

    en = render(d, "en")
    assert "not applicable" in en.lower()
    assert "no cross-run edge" in en
    # nie "Lambda = 0" / "= 0 s", in keiner Sprache (ADR-007).
    assert "= 0 s" not in en and "λ = 0" not in en


# --- Der englische Report traegt keine deutschen Formulierungen -------------------------


def test_englischer_report_ist_englisch() -> None:
    d = _bericht(DAG_STABIL, STABIL_STATS)
    en = render(d, "en")
    assert "Verdict" in en
    assert "Model limits" in en
    assert "Acceleration plan" in en
    assert "Stable" in en
    assert "λ" in en
    # keine deutschen Marker mehr im englischen Report.
    for deutsch in (
        "Urteil",
        "Modellgrenzen",
        "Kritischer Kreis",
        "halbiert",
        "Cross-Kante",
        "Takt T",
    ):
        assert deutsch not in en


def test_englisches_what_if_label_ist_englisch() -> None:
    from eigenlag.report import WhatIfTask

    result = parse_source(DAG_STABIL, "dags/takt.py")
    analysis = analyze_result(result, STABIL_STATS, "mean", None)
    d = compose(
        pfad="dags/takt.py",
        dags=result.dags,
        analysis=analysis,
        stats=STABIL_STATS,
        statistic="mean",
        takt_s=3600.0,
        takt_quelle='Schedule "@hourly"',
        dauern_quelle="Test-Fixture",
        monte_carlo=None,
        requested=[WhatIfTask(task="takt.rechne", seconds=10.0)],
    )
    en = render(d, "en")
    assert "halved" in en
    assert "removed" in en
    assert "requested" in en
    assert "halbiert" not in en


def test_gate_englischer_kommentar_ist_englisch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import subprocess

    from eigenlag.cli import main

    v1 = OHNE_KREIS
    v2 = DAG_STABIL  # bringt depends_on_past -> neue Cross-Run-Kante bei @hourly
    repo = tmp_path / "repo"
    (repo / "dags").mkdir(parents=True)
    (repo / "dags/pipeline.py").write_text(v1)

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True)

    git("init", "-q")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t.invalid", "commit", "-qm", "v1")
    git("tag", "v1")
    (repo / "dags/pipeline.py").write_text(v2)

    code = main(["check", str(repo / "dags"), "--against", "v1", "--lang", "en"])
    out = capsys.readouterr().out
    assert code == 3
    assert "triggered" in out
    assert "task units" in out
    assert "lower bound" in out
    for deutsch in ("ausgeloest", "Task-Einheiten", "Untergrenze", "Behebung"):
        assert deutsch not in out
