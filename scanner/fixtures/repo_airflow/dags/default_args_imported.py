from airflow import DAG
from airflow.operators.bash import BashOperator

from common.args import SHARED_ARGS

with DAG(
    dag_id="zeta",
    schedule="@daily",
    default_args=SHARED_ARGS,
) as dag:
    BashOperator(task_id="one", bash_command="echo 1")
