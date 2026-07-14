from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data",
    "depends_on_past": True,
    "retries": 2,
}

with DAG(
    dag_id="epsilon",
    schedule="*/15 * * * *",
    default_args=DEFAULT_ARGS,
) as dag:
    BashOperator(task_id="one", bash_command="echo 1")
    BashOperator(task_id="two", bash_command="echo 2", depends_on_past=False)
