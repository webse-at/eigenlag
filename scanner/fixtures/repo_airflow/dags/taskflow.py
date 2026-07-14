from airflow.decorators import dag, task


@dag(
    dag_id="delta",
    schedule="@hourly",
    default_args={"owner": "data", "depends_on_past": True},
)
def delta_pipeline():
    @task
    def step():
        return 1

    step()


delta_pipeline()
