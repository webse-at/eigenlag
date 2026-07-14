"""Ein DAG-Konstruktor, wie ihn gekapselte Umgebungen bauen (Vorbild: Wikimedia)."""
from airflow import DAG


class EasyDagFactory:
    def __init__(self, defaults):
        self.defaults = defaults

    def create_easy_dag(self, dag_id, **kwargs):
        # Die Schablone: dieses DAG(...) ist kein DAG des Repos, sondern der Konstruktor.
        return DAG(dag_id, default_args=self.defaults, **kwargs)


create_easy_dag = EasyDagFactory({"owner": "data"}).create_easy_dag
EasyDag = create_easy_dag  # Alias auf denselben Konstruktor


def build_operator(task_id):
    # Kein Konstruktor: gibt keinen DAG zurueck.
    return {"task_id": task_id}
