"""Tests-zuerst fuer die CLI (Spec 009, Auftrag 1).

Exit-Codes sind Vertrag: 0 = analysiert (auch instabil — das Urteil ist Sache des
Nutzers, das Gate kommt in 010), 1 = Bedienfehler, 2 = Pfad geparst, aber kein
analysierbarer DAG. Ohne jede Dauern-Quelle bricht die CLI mit Erklaerung ab,
kein stiller Default.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from eigenlag.cli import main

DAG_STABIL = """\
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="takt", schedule="@hourly") as dag:
    lade = BashOperator(task_id="lade", depends_on_past=True, bash_command="x")
    rechne = EmptyOperator(task_id="rechne")
    lade >> rechne
"""

ZWEI_DAGS = """\
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="oben", schedule="@hourly") as up:
    fertig = EmptyOperator(task_id="fertig", depends_on_past=True)

with DAG(dag_id="unten", schedule="@hourly") as down:
    warte = ExternalTaskSensor(
        task_id="warte",
        external_dag_id="oben",
        external_task_id="fertig",
        execution_delta=timedelta(hours=1),
    )
"""


def schreibe(tmp_path: Path, source: str = DAG_STABIL, name: str = "takt.py") -> str:
    dag_file = tmp_path / name
    dag_file.write_text(source)
    return str(dag_file)


def test_ohne_dauern_quelle_bedienfehler_mit_erklaerung(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["analyze", schreibe(tmp_path)])
    assert code == 1
    err = capsys.readouterr().err
    assert "--assume-duration" in err
    assert "--db" in err


def test_pfad_ohne_analysierbaren_dag_exit_2_mit_warnungen(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pfad = schreibe(tmp_path, "x = 1  # kein DAG weit und breit\n", name="leer.py")
    code = main(["analyze", pfad, "--assume-duration", "60"])
    assert code == 2
    assert "kein analysierbarer DAG" in capsys.readouterr().err


def test_dag_id_ohne_treffer_exit_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        ["analyze", schreibe(tmp_path), "--assume-duration", "60", "--dag-id", "gibtsnicht"]
    )
    assert code == 2
    assert "gibtsnicht" in capsys.readouterr().err


def test_assume_only_liefert_report_mit_urteil(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["analyze", schreibe(tmp_path), "--assume-duration", "600"])
    assert code == 0
    out = capsys.readouterr().out
    assert "Urteil" in out
    assert "Modellgrenzen" in out


def test_instabil_ist_trotzdem_exit_0(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # assume 7200 s je Task bei @hourly: Lambda 7200 > T 3600, instabil.
    code = main(["analyze", schreibe(tmp_path), "--assume-duration", "7200"])
    assert code == 0
    assert "nstabil" in capsys.readouterr().out


def test_json_ausgabe_ist_maschinenlesbar(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["analyze", schreibe(tmp_path), "--assume-duration", "600", "--json"])
    assert code == 0
    d = json.loads(capsys.readouterr().out)
    assert d["lambda_s"] == 600.0
    assert d["takt_s"] == 3600.0
    assert d["urteil"] == "stabil"
    assert d["monte_carlo"] is not None


def test_samples_0_schaltet_monte_carlo_ab(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        ["analyze", schreibe(tmp_path), "--assume-duration", "600", "--samples", "0", "--json"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["monte_carlo"] is None


def test_period_override_gewinnt(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        ["analyze", schreibe(tmp_path), "--assume-duration", "600", "--period", "500", "--json"]
    )
    assert code == 0
    d = json.loads(capsys.readouterr().out)
    assert d["takt_s"] == 500.0
    assert "--period" in d["takt_quelle"]
    assert d["urteil"] == "instabil"


def test_dag_id_filter_zieht_sensor_ziel_dag_mit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pfad = schreibe(tmp_path, ZWEI_DAGS, name="zwei.py")
    code = main(["analyze", pfad, "--assume-duration", "60", "--dag-id", "unten", "--json"])
    assert code == 0
    d = json.loads(capsys.readouterr().out)
    dag_ids = {dag["dag_id"] for dag in d["dags"]}
    assert dag_ids == {"unten", "oben"}


def test_what_if_task_und_drop_edge(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "analyze",
            schreibe(tmp_path),
            "--assume-duration",
            "600",
            "--what-if",
            "task=lade:60",
            "--what-if",
            "drop-edge=takt.lade->takt.lade",
            "--json",
        ]
    )
    assert code == 0
    d = json.loads(capsys.readouterr().out)
    angefragt = [s for s in d["what_if"] if s["angefragt"]]
    assert len(angefragt) == 2


def test_what_if_kaputte_syntax_ist_bedienfehler(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        ["analyze", schreibe(tmp_path), "--assume-duration", "600", "--what-if", "task=lade"]
    )
    assert code == 1
    assert "what-if" in capsys.readouterr().err.lower()


def test_unbekannter_what_if_task_ist_bedienfehler(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        ["analyze", schreibe(tmp_path), "--assume-duration", "600", "--what-if", "task=falsch:60"]
    )
    assert code == 1
    assert "falsch" in capsys.readouterr().err


def test_rest_ohne_token_ist_bedienfehler(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["analyze", schreibe(tmp_path), "--rest", "http://localhost:8080"])
    assert code == 1
    assert "--rest-token" in capsys.readouterr().err
