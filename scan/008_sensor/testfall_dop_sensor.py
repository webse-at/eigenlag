"""Testfall 2 (Spec 008, Verifikation): depends_on_past und ein Sensor —
operator-Spalte und duration-Semantik in task_instance."""

import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

with DAG(
    dag_id="testfall_dop_sensor",
    schedule="@hourly",
    start_date=datetime.datetime(2026, 7, 14, 18, 0, tzinfo=datetime.timezone.utc),
    catchup=True,
    max_active_runs=16,
) as dag:
    warten = TimeDeltaSensor(task_id="warten", delta=datetime.timedelta(seconds=2))
    arbeit = BashOperator(
        task_id="arbeit", bash_command="sleep 1", depends_on_past=True
    )
    warten >> arbeit
