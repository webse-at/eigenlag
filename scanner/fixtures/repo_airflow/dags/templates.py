from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="iota", schedule="@hourly") as dag:
    BashOperator(
        task_id="incremental_load",
        bash_command="load --since {{ prev_start_date_success }}",
    )
    BashOperator(
        task_id="partition_label",
        bash_command="load --day {{ prev_ds }}",
    )
