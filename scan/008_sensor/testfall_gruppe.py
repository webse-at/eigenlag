"""Testfall 1 (Spec 008, Verifikation): TaskGroup-Prefix in task_instance.task_id."""

import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="testfall_gruppe",
    schedule="@hourly",
    start_date=datetime.datetime(2026, 7, 14, 18, 0, tzinfo=datetime.timezone.utc),
    catchup=True,
    max_active_runs=16,
) as dag:
    start = BashOperator(task_id="start", bash_command="sleep 1")
    with TaskGroup("grp") as grp:
        laden = BashOperator(task_id="laden", bash_command="sleep 2")
    ende = BashOperator(task_id="ende", bash_command="sleep 1")
    start >> grp >> ende
