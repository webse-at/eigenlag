"""Tests fuer analyze(): die Kompositionskette parse -> Dauern -> condense -> Howard.

Werte von Hand: der Fixture-DAG unten hat eine depends_on_past-Selbstkante an `wait`.
Kritischer Kreis kondensiert: wait -> wait, Gewicht = Dauer von wait, periods = 1,
also lambda = Dauer von wait. Critical Path = wait + work.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from eigenlag.analyze import SENSOR_CYCLE_TEXT, analyze
from eigenlag.durations import TaskStats, assume

SENSOR_DAG = """\
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="mit_sensor", schedule="@hourly") as dag:
    wait = HttpSensor(task_id="wait", depends_on_past=True)
    work = EmptyOperator(task_id="work")
    wait >> work
"""

OHNE_KREIS_DAG = """\
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(dag_id="ohne_kreis", schedule="@hourly") as dag:
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    a >> b
"""


def stats_mit_sensor() -> dict[str, TaskStats]:
    return {
        "mit_sensor.wait": TaskStats(p50=280.0, p95=340.0, mean=300.0, n=12, operator="HttpSensor"),
        "mit_sensor.work": TaskStats(p50=55.0, p95=80.0, mean=60.0, n=12, operator="EmptyOperator"),
    }


def test_analyze_lambda_in_sekunden_und_kritischer_kreis(tmp_path: Path) -> None:
    dag_file = tmp_path / "mit_sensor.py"
    dag_file.write_text(SENSOR_DAG)
    result = analyze(dag_file, stats_mit_sensor())
    assert result.lam == 300.0
    assert result.cycle is not None
    assert [(e.src, e.dst, e.periods) for e in result.cycle] == [
        ("mit_sensor.wait", "mit_sensor.wait", 1)
    ]
    assert result.cycle_tasks == ("mit_sensor.wait",)
    assert result.critical_path_s == 360.0
    assert result.critical_path_tasks == ["mit_sensor.wait", "mit_sensor.work"]


def test_analyze_sensor_im_kritischen_kreis_erzeugt_pflicht_warnung(tmp_path: Path) -> None:
    dag_file = tmp_path / "mit_sensor.py"
    dag_file.write_text(SENSOR_DAG)
    result = analyze(dag_file, stats_mit_sensor())
    sensor_warnings = [w for w in result.warnings if w.kind == "sensor_im_kritischen_kreis"]
    assert len(sensor_warnings) == 1
    assert sensor_warnings[0].task == "mit_sensor.wait"
    assert sensor_warnings[0].detail == SENSOR_CYCLE_TEXT


def test_analyze_andere_statistik_und_kein_sensor_keine_warnung(tmp_path: Path) -> None:
    dag_file = tmp_path / "mit_sensor.py"
    dag_file.write_text(SENSOR_DAG)
    stats = stats_mit_sensor()
    stats["mit_sensor.wait"] = TaskStats(
        p50=280.0, p95=340.0, mean=300.0, n=12, operator="PythonOperator"
    )
    result = analyze(dag_file, stats, statistic="p95")
    assert result.lam == 340.0
    assert [w for w in result.warnings if w.kind == "sensor_im_kritischen_kreis"] == []


def test_analyze_ohne_kreis_hat_kein_lambda(tmp_path: Path) -> None:
    dag_file = tmp_path / "ohne_kreis.py"
    dag_file.write_text(OHNE_KREIS_DAG)
    result = analyze(dag_file, {}, fallback=assume(10.0))
    assert result.lam is None
    assert result.cycle is None
    assert result.cycle_tasks == ()
    assert result.critical_path_s == 20.0
    # Mischbetrieb-Warnungen der Dauern-Schicht laufen durch:
    assert {w.kind for w in result.warnings} == {"dauer_angenommen"}


def test_analyze_ohne_fallback_und_ohne_messung_wirft(tmp_path: Path) -> None:
    dag_file = tmp_path / "ohne_kreis.py"
    dag_file.write_text(OHNE_KREIS_DAG)
    with pytest.raises(ValueError, match="ohne_kreis.a"):
        analyze(dag_file, {})
