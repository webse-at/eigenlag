"""Dieser DAG benutzt depends_on_past nicht.

wait_for_downstream=True waere hier fachlich falsch, siehe Doku.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator

# depends_on_past=True stand hier frueher, wurde absichtlich entfernt
DOCS_URL = "https://example.org/why-depends_on_past=True-is-wrong"

with DAG(dag_id="theta", schedule="@hourly") as dag:
    BashOperator(
        task_id="noop",
        bash_command="echo 'depends_on_past=True'",
        depends_on_past=False,
    )
