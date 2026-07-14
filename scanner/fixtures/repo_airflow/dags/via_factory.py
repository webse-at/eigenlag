"""DAGs, die nicht ueber DAG(...) entstehen, sondern ueber den Konstruktor des Repos."""
from datetime import datetime

from airflow.operators.bash import BashOperator

from utils.easy_dag import EasyDag, create_easy_dag

with create_easy_dag(
    dag_id="kappa",
    schedule="0 */2 * * *",
    default_args={"depends_on_past": True},
    start_date=datetime(2024, 1, 1),
) as kappa:
    BashOperator(task_id="load", bash_command="echo load")


with EasyDag(dag_id="lambda_dag", schedule="@daily") as lambda_dag:
    BashOperator(task_id="report", bash_command="echo report")


def build_dag(dag_id):
    # Kein Konstruktor (ADR-015, Punkt 3): reicht nur weiter, was der Konstruktor liefert.
    # Der DAG entsteht in der Zeile darunter, dort stehen Schedule und default_args.
    with create_easy_dag(
        dag_id=dag_id,
        schedule="@hourly",
        default_args={"depends_on_past": True},
    ) as generated:
        BashOperator(task_id="work", bash_command="echo work")
    return generated


my_dag = build_dag("mu")
