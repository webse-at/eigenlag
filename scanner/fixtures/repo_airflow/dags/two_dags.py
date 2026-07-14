from airflow import DAG
from airflow.operators.bash import BashOperator

dag_a = DAG(
    dag_id="alpha",
    schedule="0 */6 * * *",
)

dag_b = DAG(
    dag_id="beta",
    schedule="@daily",
)

extract = BashOperator(
    task_id="extract",
    bash_command="echo alpha",
    depends_on_past=True,
    dag=dag_a,
)

load = BashOperator(
    task_id="load",
    bash_command="echo beta",
    depends_on_past=False,
    dag=dag_b,
)

orphan = BashOperator(
    task_id="orphan",
    bash_command="echo orphan",
    wait_for_downstream=True,
)
