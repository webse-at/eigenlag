from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"wait_for_downstream": True}

with DAG(dag_id="feature_pipeline", schedule="@hourly", default_args=default_args) as dag:
    build_features = BashOperator(task_id="build_features", bash_command="x")
    train_model = BashOperator(task_id="train_model", bash_command="x")
    build_features >> train_model
