from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def kafka_consumer_pod_operator(
    task_id: str,
    image: str,
    depends_on_past: bool = True,
    wait_for_downstream: bool = True,
):
    """Factory: jeder Task hieraus traegt beide starken Signale."""
    return KubernetesPodOperator(
        task_id=task_id,
        image=image,
        depends_on_past=depends_on_past,
        wait_for_downstream=wait_for_downstream,
    )


def strict_bash_operator(task_id: str, command: str):
    return BashOperator(task_id=task_id, bash_command=command, depends_on_past=True)


def plain_pod_operator(task_id: str, image: str):
    return KubernetesPodOperator(task_id=task_id, image=image)
