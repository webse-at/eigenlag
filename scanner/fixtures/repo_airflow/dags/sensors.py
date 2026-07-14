from datetime import timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor


def yesterday(logical_date):
    return logical_date - timedelta(days=1)


with DAG(dag_id="eta", schedule="0 6,18 * * *") as dag:
    ExternalTaskSensor(
        task_id="wait_same_interval",
        external_dag_id="upstream",
        external_task_id="done",
    )
    ExternalTaskSensor(
        task_id="wait_previous_interval",
        external_dag_id="upstream",
        execution_delta=timedelta(hours=6),
    )
    ExternalTaskSensor(
        task_id="wait_via_fn",
        external_dag_id="upstream",
        execution_date_fn=yesterday,
    )
    ExternalTaskSensor(
        task_id="wait_prior_dates",
        external_dag_id="upstream",
        include_prior_dates=True,
    )
