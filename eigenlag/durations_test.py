"""Tests fuer die Dauern-Schicht (Spec 008). Statistik-Pins von Hand nachgerechnet:

[10, 20, 30, 40, 50]: mean = 30, p50 = 30 (Index 0.5*4 = 2),
p95 = Index 0.95*4 = 3.8 -> 40 + 0.8*(50-40) = 48 (lineare Interpolation,
identisch mit percentile_cont).
[1, 2, 3, 4, 5]: mean = 3, p50 = 3, p95 = Index 3.8 -> 4 + 0.8 = 4.8.
[2, 4]: mean = 3, p50 = 3 (Index 0.5), p95 = 2 + 0.95*2 = 3.9.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest

from eigenlag.durations import (
    DurationWarning,
    TaskStats,
    _auth_header,
    _percentile,
    assume,
    from_metadata_db,
    from_rest,
    pick,
    resolve,
)

# --- Perzentil (muss percentile_cont entsprechen: lineare Interpolation) -------------


def test_percentile_interpolation_von_hand() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    assert _percentile(values, 0.5) == 30.0
    assert _percentile(values, 0.95) == pytest.approx(48.0)
    assert _percentile([2.0, 4.0], 0.5) == 3.0
    assert _percentile([2.0, 4.0], 0.95) == pytest.approx(3.9)
    assert _percentile([7.0], 0.5) == 7.0
    assert _percentile([7.0], 0.95) == 7.0


# --- pick und assume ------------------------------------------------------------------


def stats_fixture() -> dict[str, TaskStats]:
    return {
        "d.a": TaskStats(p50=10.0, p95=19.0, mean=12.0, n=8, operator="PythonOperator"),
        "d.b": TaskStats(p50=5.0, p95=9.0, mean=6.0, n=20, operator="ExternalTaskSensor"),
    }


def test_pick_waehlt_die_statistik() -> None:
    stats = stats_fixture()
    assert pick(stats, "mean") == {"d.a": 12.0, "d.b": 6.0}
    assert pick(stats, "p50") == {"d.a": 10.0, "d.b": 5.0}
    assert pick(stats, "p95") == {"d.a": 19.0, "d.b": 9.0}


def test_is_sensor_kommt_aus_dem_operator_namen() -> None:
    stats = stats_fixture()
    assert stats["d.a"].is_sensor is False
    assert stats["d.b"].is_sensor is True
    # Async-Varianten enthalten "Sensor", enden aber nicht darauf.
    assert TaskStats(p50=1, p95=1, mean=1, n=5, operator="DateTimeSensorAsync").is_sensor
    assert not TaskStats(p50=1, p95=1, mean=1, n=5, operator=None).is_sensor


def test_assume_liefert_dieselbe_struktur() -> None:
    a = assume(300.0)
    assert (a.p50, a.p95, a.mean, a.n) == (300.0, 300.0, 300.0, 0)
    assert a.operator is None
    assert a.is_sensor is False


# --- resolve: Mischbetrieb und Mindest-Stichprobe -------------------------------------


def test_resolve_vollstaendig_ohne_warnung() -> None:
    durations, warnings = resolve(["d.a", "d.b"], stats_fixture(), "mean", fallback=None)
    assert durations == {"d.a": 12.0, "d.b": 6.0}
    assert warnings == ()


def test_resolve_mischbetrieb_fehlende_task_bekommt_assume_mit_warnung() -> None:
    durations, warnings = resolve(
        ["d.a", "d.b", "d.neu"], stats_fixture(), "mean", fallback=assume(300.0)
    )
    assert durations == {"d.a": 12.0, "d.b": 6.0, "d.neu": 300.0}
    assert warnings == (
        DurationWarning(kind="dauer_angenommen", task="d.neu", detail="no measurement, 300.0 s"),
    )


def test_resolve_unter_mindest_stichprobe_faellt_auf_assume() -> None:
    stats = {"d.a": TaskStats(p50=10.0, p95=19.0, mean=12.0, n=4, operator=None)}
    durations, warnings = resolve(["d.a"], stats, "p95", fallback=assume(60.0))
    assert durations == {"d.a": 60.0}
    assert [w.kind for w in warnings] == ["stichprobe_zu_klein"]
    assert warnings[0].task == "d.a"
    assert "n=4 < 5" in warnings[0].detail


def test_resolve_ohne_fallback_wirft_bei_fehlender_task() -> None:
    with pytest.raises(ValueError, match="d.neu"):
        resolve(["d.a", "d.neu"], stats_fixture(), "mean", fallback=None)


# --- from_metadata_db gegen eine SQLite-Fixture-DB ------------------------------------


def build_fixture_db(path: Path) -> str:
    """task_instance-Zeilen von Hand: Werte siehe Modul-Docstring."""
    import sqlalchemy as sa

    url = f"sqlite:///{path}"
    engine = sa.create_engine(url)
    now = dt.datetime.now(dt.UTC)
    # Airflows SQLite-Format: naiver UTC-String ohne Offset (gegen 3.3.0 verifiziert).
    fresh = (now - dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.%f")
    stale = (now - dt.timedelta(days=400)).strftime("%Y-%m-%d %H:%M:%S.%f")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE task_instance (dag_id TEXT, task_id TEXT, state TEXT,"
                " duration FLOAT, operator TEXT, start_date TIMESTAMP)"
            )
        )
        rows: list[tuple[str, str, str, float | None, str | None, str]] = []
        for d in [10.0, 20.0, 30.0, 40.0, 50.0]:
            rows.append(("etl", "extract", "success", d, "PythonOperator", fresh))
        for d in [1.0, 2.0, 3.0, 4.0, 5.0]:
            rows.append(("etl", "grp.load", "success", d, "PythonOperator", fresh))
        for _ in range(5):
            rows.append(("etl", "wait", "success", 60.0, "ExternalTaskSensor", fresh))
        rows.append(("etl", "rare", "success", 2.0, None, fresh))
        rows.append(("etl", "rare", "success", 4.0, None, fresh))
        # Muss alles herausgefiltert werden:
        rows.append(("etl", "extract", "failed", 999.0, "PythonOperator", fresh))
        rows.append(("etl", "extract", "success", None, "PythonOperator", fresh))
        rows.append(("etl", "extract", "success", 99999.0, "PythonOperator", stale))
        rows.append(("anderer", "extract", "success", 777.0, "PythonOperator", fresh))
        conn.execute(
            sa.text(
                "INSERT INTO task_instance (dag_id, task_id, state, duration, operator,"
                " start_date) VALUES (:d, :t, :s, :dur, :op, :sd)"
            ),
            [
                {"d": d, "t": t, "s": s, "dur": dur, "op": op, "sd": sd}
                for (d, t, s, dur, op, sd) in rows
            ],
        )
    return url


def test_from_metadata_db_aggregiert_und_filtert(tmp_path: Path) -> None:
    url = build_fixture_db(tmp_path / "airflow.db")
    stats, warnings = from_metadata_db(url, ["etl"], since_days=90)
    assert warnings == ()
    assert set(stats) == {"etl.extract", "etl.grp.load", "etl.wait", "etl.rare"}

    extract = stats["etl.extract"]
    assert (extract.n, extract.mean, extract.p50) == (5, 30.0, 30.0)
    assert extract.p95 == pytest.approx(48.0)
    assert extract.operator == "PythonOperator"
    assert extract.is_sensor is False

    load = stats["etl.grp.load"]
    assert (load.n, load.mean, load.p50) == (5, 3.0, 3.0)
    assert load.p95 == pytest.approx(4.8)

    assert stats["etl.wait"].is_sensor is True
    assert stats["etl.rare"].n == 2
    assert stats["etl.rare"].mean == 3.0


def test_from_metadata_db_taskgroup_prefix_passt_zum_parser_namespacing(
    tmp_path: Path,
) -> None:
    """Pin: DB-task_id traegt den TaskGroup-Prefix, der Knoten heisst dag_id.gruppe.task —
    exakt der Name, den to_pipeline aus dem Parser erzeugt."""
    from eigenlag.parse_airflow import parse_source, to_pipeline

    url = build_fixture_db(tmp_path / "airflow.db")
    stats, _ = from_metadata_db(url, ["etl"], since_days=90)

    source = (
        "from airflow import DAG\n"
        "from airflow.operators.empty import EmptyOperator\n"
        "from airflow.utils.task_group import TaskGroup\n"
        "with DAG(dag_id='etl') as dag:\n"
        "    extract = EmptyOperator(task_id='extract')\n"
        "    with TaskGroup('grp') as grp:\n"
        "        load = EmptyOperator(task_id='load')\n"
        "    wait = EmptyOperator(task_id='wait')\n"
        "    rare = EmptyOperator(task_id='rare')\n"
        "    extract >> grp >> wait >> rare\n"
    )
    dags = parse_source(source, "dags/etl.py").dags
    durations, warnings = resolve(
        [f"etl.{t}" for t in dags[0].tasks], stats, "mean", fallback=assume(1.0)
    )
    pipeline = to_pipeline(dags, durations)
    assert pipeline.durations["etl.grp.load"] == 3.0
    assert pipeline.durations["etl.extract"] == 30.0
    # rare hat n=2 < 5 und faellt auf den Assume-Wert:
    assert pipeline.durations["etl.rare"] == 1.0
    assert [w.kind for w in warnings] == ["stichprobe_zu_klein"]


# --- from_rest gegen gespeicherte JSON-Fixtures ---------------------------------------


def test_auth_header() -> None:
    assert _auth_header(("user", "pw")) == "Basic dXNlcjpwdw=="
    assert _auth_header("ein-token") == "Bearer ein-token"


def ti(
    task_id: str, state: str, duration: float | None, operator: str = "BashOperator"
) -> dict[str, Any]:
    return {"task_id": task_id, "state": state, "duration": duration, "operator": operator}


def test_from_rest_paginiert_filtert_und_aggregiert(monkeypatch: pytest.MonkeyPatch) -> None:
    page1 = {
        "task_instances": [ti("a", "success", d) for d in [10.0, 20.0]]
        + [ti("a", "failed", 999.0), ti("a", "success", None)],
        "total_entries": 7,
    }
    page2 = {
        "task_instances": [ti("a", "success", d) for d in [30.0, 40.0, 50.0]],
        "total_entries": 7,
    }
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_get(url: str, headers: dict[str, str]) -> Any:
        calls.append((url, headers))
        return json.loads(json.dumps(page1 if "offset=0" in url else page2))

    monkeypatch.setattr("eigenlag.durations._get_json", fake_get)
    # Airflow-2-Pfad: /api/v1 mit Basic Auth. Airflow 3 (Default v2, Bearer) unten.
    stats, warnings = from_rest(
        "http://af:8080",
        ("admin", "pw"),
        ["etl"],
        page_size=4,
        min_interval_s=0.0,
        api_version="v1",
    )
    assert warnings == ()
    assert len(calls) == 2
    assert all(h["Authorization"] == "Basic YWRtaW46cHc=" for _, h in calls)
    assert "/api/v1/dags/etl/dagRuns/~/taskInstances" in calls[0][0]
    a = stats["etl.a"]
    assert (a.n, a.mean, a.p50) == (5, 30.0, 30.0)
    assert a.p95 == pytest.approx(48.0)


def test_from_rest_seiten_deckel_warnt_statt_endlos(monkeypatch: pytest.MonkeyPatch) -> None:
    page = {"task_instances": [ti("a", "success", 1.0)], "total_entries": 1000}
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_get(url: str, headers: dict[str, str]) -> Any:
        calls.append((url, headers))
        return json.loads(json.dumps(page))

    monkeypatch.setattr("eigenlag.durations._get_json", fake_get)
    stats, warnings = from_rest(
        "http://af:8080", "tok", ["etl"], page_size=1, max_pages=3, min_interval_s=0.0
    )
    # Default ist der Airflow-3-Pfad: /api/v2 mit Bearer-Token.
    assert "/api/v2/dags/etl/dagRuns/~/taskInstances" in calls[0][0]
    assert calls[0][1]["Authorization"] == "Bearer tok"
    assert stats["etl.a"].n == 3
    assert [w.kind for w in warnings] == ["rest_seiten_deckel"]
    assert "etl" in warnings[0].detail
