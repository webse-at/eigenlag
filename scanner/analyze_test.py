from pathlib import Path

import pytest

from scanner.analyze import (
    DagFinding,
    RepoAnalysis,
    analyze_repo,
    analyze_source,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "repo_airflow"


@pytest.fixture(scope="module")
def result() -> RepoAnalysis:
    return analyze_repo(FIXTURE, "acme/fixture")


def dag(result: RepoAnalysis, dag_id: str) -> DagFinding:
    matches = [d for d in result.dags if d.dag_id == dag_id]
    assert len(matches) == 1, f"{dag_id} nicht genau einmal gefunden: {result.dags}"
    return matches[0]


def kinds(finding: DagFinding) -> set[str]:
    return {s.kind for s in finding.signals}


def test_all_dags_are_found_exactly_once(result: RepoAnalysis) -> None:
    assert sorted(d.dag_id or "" for d in result.dags) == [
        "alpha",
        "beta",
        "delta",
        "epsilon",
        "eta",
        "gamma",
        "iota",
        "theta",
        "zeta",
    ]


def test_variable_assignment_scopes_the_signal_to_the_right_dag(result: RepoAnalysis) -> None:
    alpha = dag(result, "alpha")
    assert kinds(alpha) == {"depends_on_past"}
    signal = alpha.signals[0]
    assert (signal.file, signal.lineno) == ("dags/two_dags.py", 17)
    assert signal.source == "operator"
    assert signal.inferred is False


def test_depends_on_past_false_is_not_a_signal(result: RepoAnalysis) -> None:
    assert dag(result, "beta").signals == []


def test_task_without_dag_reference_in_multi_dag_file_is_not_guessed(result: RepoAnalysis) -> None:
    assert "wait_for_downstream" not in kinds(dag(result, "alpha"))
    assert "wait_for_downstream" not in kinds(dag(result, "beta"))
    ambiguous = [e for e in result.errors if e["kind"] == "ambiguous_task"]
    assert len(ambiguous) == 1
    # Anker ist der Operator-Aufruf (Zeile 28), nicht das Signal-Keyword: ambig ist der Task.
    assert (ambiguous[0]["file"], ambiguous[0]["lineno"]) == ("dags/two_dags.py", 28)
    assert ambiguous[0]["signals"] == ["wait_for_downstream"]


def test_with_dag_body_scopes_the_signal(result: RepoAnalysis) -> None:
    gamma = dag(result, "gamma")
    assert kinds(gamma) == {"wait_for_downstream"}
    assert (gamma.signals[0].file, gamma.signals[0].lineno) == ("dags/with_dag.py", 15)
    assert gamma.schedule == "subdaily"


def test_dag_decorator_and_default_args_in_the_dag_call(result: RepoAnalysis) -> None:
    delta = dag(result, "delta")
    assert kinds(delta) == {"depends_on_past"}
    assert delta.signals[0].source == "default_args"
    assert (delta.signals[0].file, delta.signals[0].lineno) == ("dags/taskflow.py", 7)
    assert delta.schedule == "subdaily"


def test_default_args_from_a_dict_literal_variable_is_resolved(result: RepoAnalysis) -> None:
    epsilon = dag(result, "epsilon")
    assert kinds(epsilon) == {"depends_on_past"}
    assert (epsilon.signals[0].file, epsilon.signals[0].lineno) == ("dags/default_args.py", 6)
    assert epsilon.signals[0].source == "default_args"
    assert epsilon.schedule == "subdaily"


def test_imported_default_args_are_not_guessed(result: RepoAnalysis) -> None:
    assert dag(result, "zeta").signals == []
    unresolved = [e for e in result.errors if e["kind"] == "unresolved_default_args"]
    assert len(unresolved) == 1
    assert unresolved[0]["file"] == "dags/default_args_imported.py"


def test_external_task_sensor_needs_a_time_offset(result: RepoAnalysis) -> None:
    eta = dag(result, "eta")
    lines = sorted((s.kind, s.lineno) for s in eta.signals)
    assert lines == [
        ("external_task_sensor", 20),
        ("external_task_sensor", 25),
        ("include_prior_dates", 30),
    ]
    assert eta.schedule == "subdaily"


def test_comments_docstrings_and_strings_are_not_signals(result: RepoAnalysis) -> None:
    assert dag(result, "theta").signals == []


def test_prior_run_templates_are_split_into_strong_and_weak(result: RepoAnalysis) -> None:
    iota = dag(result, "iota")
    assert sorted((s.kind, s.lineno) for s in iota.signals) == [
        ("prev_run_date", 11),
        ("prev_run_success", 7),
    ]


def test_syntax_error_is_logged_and_does_not_crash(result: RepoAnalysis) -> None:
    errors = [e for e in result.errors if e["kind"] == "syntax_error"]
    assert len(errors) == 1
    assert errors[0]["file"] == "dags/broken.py"
    assert result.syntax_errors == 1


def test_factories_are_counted_separately(result: RepoAnalysis) -> None:
    assert sorted((s.kind, s.lineno) for s in result.factories) == [
        ("depends_on_past", 8),
        ("depends_on_past", 21),
        ("wait_for_downstream", 9),
    ]
    assert {s.file for s in result.factories} == {"plugins/kafka_factory.py"}
    assert all(s not in result.factories for finding in result.dags for s in finding.signals)


def test_helper_returning_a_dict_is_not_a_factory(result: RepoAnalysis) -> None:
    assert not [s for s in result.factories if s.file == "utils/helpers.py"]


def test_risk_candidate_needs_a_strong_signal_and_a_subdaily_schedule(result: RepoAnalysis) -> None:
    assert sorted(d.dag_id or "" for d in result.dags if d.is_risk_candidate) == [
        "alpha",
        "delta",
        "epsilon",
        "eta",
        "gamma",
        "iota",
    ]
    assert dag(result, "beta").is_risk_candidate is False  # daily, kein Signal
    assert dag(result, "theta").is_risk_candidate is False  # subdaily, kein Signal
    assert dag(result, "zeta").is_risk_candidate is False  # Signal unauflösbar


def test_weak_template_alone_is_no_risk_candidate() -> None:
    source = """
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="weak_only", schedule="@hourly") as dag:
    BashOperator(task_id="t", bash_command="echo {{ prev_ds }}")
"""
    analysis = analyze_source(source, "dags/weak.py")
    assert kinds(analysis.dags[0]) == {"prev_run_date"}
    assert analysis.dags[0].is_risk_candidate is False


def test_single_dag_file_infers_the_owner_of_an_unbound_task() -> None:
    source = """
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(dag_id="solo", schedule="@hourly")

BashOperator(task_id="t", bash_command="echo 1", depends_on_past=True)
"""
    analysis = analyze_source(source, "dags/solo.py")
    solo = analysis.dags[0]
    assert kinds(solo) == {"depends_on_past"}
    assert solo.signals[0].inferred is True
    assert solo.is_risk_candidate is True


def test_dag_id_from_a_module_level_string_variable_is_resolved() -> None:
    # Muster aus mozilla/telemetry-airflow, dags/copy_deduplicate.py:60/63.
    source = """
from airflow import models

dag_name = "copy_deduplicate"

with models.DAG(dag_name, schedule_interval="0 */2 * * *") as dag:
    bigquery_etl_query(task_id="q", depends_on_past=True, dag=dag)
"""
    analysis = analyze_source(source, "dags/copy_deduplicate.py")
    assert analysis.dags[0].dag_id == "copy_deduplicate"
    assert analysis.dags[0].is_risk_candidate is True


def test_dag_id_from_an_unresolvable_expression_stays_empty() -> None:
    source = """
from airflow import DAG

with DAG(f"etl_{env}", schedule="@hourly") as dag:
    pass
"""
    analysis = analyze_source(source, "dags/dynamic.py")
    assert analysis.dags[0].dag_id is None


def test_tasks_are_counted_per_dag(result: RepoAnalysis) -> None:
    counts = {d.dag_id: d.task_count for d in result.dags}
    assert counts == {
        "alpha": 1,  # extract, ueber dag= gebunden
        "beta": 1,  # load, ueber dag= gebunden
        "gamma": 1,
        "delta": 1,  # @task-Funktion, kein Operator-Aufruf
        "epsilon": 2,
        "zeta": 1,
        "eta": 4,
        "theta": 1,
        "iota": 2,
    }


def test_an_unassignable_task_is_not_counted_for_any_dag(result: RepoAnalysis) -> None:
    # `orphan` in dags/two_dags.py:28 haengt an keinem der beiden DAGs. Geraten wird nicht.
    assert dag(result, "alpha").task_count + dag(result, "beta").task_count == 2


def test_factory_operators_are_not_counted_as_tasks(result: RepoAnalysis) -> None:
    assert all(finding.file != "plugins/kafka_factory.py" for finding in result.dags)


def test_taskflow_decorator_variants_count(result: RepoAnalysis) -> None:
    source = """
from airflow.decorators import dag, task

@dag(dag_id="omega", schedule="@hourly")
def pipeline():
    @task
    def a():
        return 1

    @task.bash
    def b():
        return "echo 1"

    a() >> b()
"""
    analysis = analyze_source(source, "dags/omega.py")
    assert analysis.dags[0].task_count == 2


def test_execution_delta_of_zero_is_no_cross_run_signal() -> None:
    # Gefunden in der Stichprobe zu Session 003: Dat-Al/Fidai,
    # airflow/dags/predict_hourly_dag.py:37, `execution_delta=timedelta(hours=0)`, im Code
    # kommentiert mit "regarde la meme heure d'execution". Nullversatz zeigt auf denselben
    # Logical Date, das ist eine Intra-Run-Kante (wiki/signals.md, Signal C).
    source = """
from datetime import timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="zero_delta", schedule="0 * * * *") as dag:
    ExternalTaskSensor(
        task_id="wait",
        external_dag_id="upstream",
        execution_delta=timedelta(hours=0),
    )
"""
    analysis = analyze_source(source, "dags/zero_delta.py")
    assert analysis.dags[0].signals == []
    assert analysis.dags[0].is_risk_candidate is False


def test_execution_delta_that_cannot_be_resolved_still_counts() -> None:
    # Ein Versatz aus einer Variablen ist statisch nicht bestimmbar. Sein einziger Zweck ist
    # der Zeitversatz, deshalb zaehlt er weiter (wiki/signals.md, Signal C, "Grenze").
    source = """
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="var_delta", schedule="0 * * * *") as dag:
    ExternalTaskSensor(task_id="wait", external_dag_id="up", execution_delta=OFFSET)
"""
    analysis = analyze_source(source, "dags/var_delta.py")
    assert kinds(analysis.dags[0]) == {"external_task_sensor"}


def test_context_parameter_of_a_callable_is_signal_f() -> None:
    # Muster aus V-Dang/covid_pipeline und ZinyProxy/Product: Airflow injiziert den Kontext
    # ueber den Parameternamen. Das ist dieselbe Wartesemantik wie das Template.
    source = """
from airflow import DAG
from airflow.operators.python import PythonOperator


def last_success(prev_start_date_success, **kwargs):
    return prev_start_date_success


with DAG(dag_id="ctx", schedule="@hourly") as dag:
    PythonOperator(task_id="t", python_callable=last_success)
    PythonOperator(
        task_id="check",
        python_callable=lambda prev_start_date_success: prev_start_date_success is not None,
    )
"""
    analysis = analyze_source(source, "dags/ctx.py")
    signals = analysis.dags[0].signals
    assert {s.kind for s in signals} == {"prev_run_success"}
    assert {s.source for s in signals} == {"context_param"}
    assert analysis.dags[0].is_risk_candidate is True


def test_weak_context_parameter_stays_weak() -> None:
    source = """
from airflow import DAG
from airflow.operators.python import PythonOperator


def partition(prev_ds, **kwargs):
    return prev_ds


with DAG(dag_id="weak_ctx", schedule="@hourly") as dag:
    PythonOperator(task_id="t", python_callable=partition)
"""
    analysis = analyze_source(source, "dags/weak_ctx.py")
    assert kinds(analysis.dags[0]) == {"prev_run_date"}
    assert analysis.dags[0].is_risk_candidate is False


def test_template_in_a_module_variable_is_signal_f() -> None:
    # Muster aus abdurahim-dag/portfolio: das Template steht in einer Modul-Variablen,
    # nicht im Operator-Argument.
    source = """
from airflow import DAG
from airflow.operators.bash import BashOperator

date_last_success = '{{ prev_start_date_success }}'

with DAG(dag_id="modvar", schedule="@hourly") as dag:
    BashOperator(task_id="t", bash_command=f"load --since {date_last_success}")
"""
    analysis = analyze_source(source, "dags/modvar.py")
    signal = analysis.dags[0].signals[0]
    assert (signal.kind, signal.source, signal.lineno) == ("prev_run_success", "module_template", 5)


def test_context_parameter_without_a_dag_in_the_file_is_no_signal() -> None:
    # Muster aus V-Dang/covid_pipeline, archive.py: Helfer ohne DAG im File. Ohne DAG-Scope
    # wird nichts geraten (Regel 5).
    source = """
def get_last_execution_date(prev_start_date_success, **kwargs):
    return prev_start_date_success
"""
    analysis = analyze_source(source, "utils/archive.py")
    assert analysis.dags == []
    assert analysis.factories == []


def test_context_parameter_in_a_multi_dag_file_is_not_guessed() -> None:
    source = """
from airflow import DAG
from airflow.operators.python import PythonOperator

dag_a = DAG(dag_id="a", schedule="@hourly")
dag_b = DAG(dag_id="b", schedule="@hourly")


def last_success(prev_start_date_success, **kwargs):
    return prev_start_date_success
"""
    analysis = analyze_source(source, "dags/two.py")
    assert all(not d.signals for d in analysis.dags)
    assert [e["kind"] for e in analysis.errors] == ["ambiguous_task"]


def test_paths_are_never_truncated(result: RepoAnalysis) -> None:
    for finding in result.dags:
        for signal in finding.signals:
            assert (FIXTURE / signal.file).exists()
            assert signal.lineno > 0
