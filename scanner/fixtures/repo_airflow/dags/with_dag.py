from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run() -> None:
    pass


with DAG(dag_id="gamma", schedule=timedelta(hours=4)) as dag:
    PythonOperator(
        task_id="single",
        python_callable=run,
        wait_for_downstream=True,
    )
