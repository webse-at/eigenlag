"""Tests-zuerst fuer den Airflow-Parser (Spec 007).

Der Kern der Session ist die Uebersetzungstabelle Signal -> Kante. Jede Tabellen-Zeile
ist hier ein Test, geschrieben bevor der Parser existiert. Die Leitregel: der Parser
darf weniger wissen, als im File steht, aber nie mehr — was nicht statisch aufloesbar
ist, wird Warnung, nicht Kante.
"""

from __future__ import annotations

from textwrap import dedent

from eigenlag.parse_airflow import (
    ParsedCrossEdge,
    ParsedDag,
    parse_source,
    to_pipeline,
)

HEADER = """\
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
"""


def parse(source: str, path: str = "dags/x.py") -> tuple[ParsedDag, ...]:
    return parse_source(HEADER + dedent(source), path).dags


def parse_one(source: str, path: str = "dags/x.py") -> ParsedDag:
    dags = parse(source, path)
    assert len(dags) == 1, dags
    return dags[0]


def edges(dag: ParsedDag) -> set[tuple[str, str, int, str]]:
    return {(e.src, e.dst, e.periods, e.signal) for e in dag.cross}


def warning_kinds(dag: ParsedDag) -> set[str]:
    return {w.kind for w in dag.warnings}


# --- Task- und Kanten-Erkennung ------------------------------------------------------


def test_tasks_aus_operator_instanzen_mit_statischem_task_id() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = BashOperator(task_id="extract", bash_command="x")
            b = EmptyOperator(task_id="noop")
        """
    )
    assert dag.dag_id == "d"
    assert set(dag.tasks) == {"extract", "noop"}


def test_task_dekorierte_funktionen_sind_tasks() -> None:
    source = dedent(
        """
        from airflow.decorators import dag, task

        @dag(dag_id="tf", schedule="@hourly")
        def tf():
            @task
            def pull():
                return 1

            @task.bash
            def push():
                return "echo"

            pull()
            push()

        tf()
        """
    )
    result = parse_source(source, "dags/tf.py")
    assert len(result.dags) == 1
    assert set(result.dags[0].tasks) == {"pull", "push"}


def test_kanten_gekettet_und_mit_listen() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            d = EmptyOperator(task_id="d")
            a >> b >> [c, d]
        """
    )
    assert set(dag.intra) == {("a", "b"), ("b", "c"), ("b", "d")}


def test_kanten_rueckwaerts_und_liste_links() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            c << [a, b]
        """
    )
    assert set(dag.intra) == {("a", "c"), ("b", "c")}


def test_set_upstream_und_set_downstream() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            b.set_upstream(a)
            b.set_downstream([c])
        """
    )
    assert set(dag.intra) == {("a", "b"), ("b", "c")}


def test_chain_mit_listen_paart_elementweise() -> None:
    dag = parse_one(
        """
        from airflow.models.baseoperator import chain

        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            d = EmptyOperator(task_id="d")
            e = EmptyOperator(task_id="e")
            chain(a, [b, c], [d, e])
        """
    )
    assert set(dag.intra) == {("a", "b"), ("a", "c"), ("b", "d"), ("c", "e")}


def test_taskgroup_prefix_namespace_und_kanten_durch_die_gruppe() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            start = EmptyOperator(task_id="start")
            with TaskGroup("grp") as grp:
                x = EmptyOperator(task_id="x")
                y = EmptyOperator(task_id="y")
                x >> y
            end = EmptyOperator(task_id="end")
            start >> grp >> end
        """
    )
    assert set(dag.tasks) == {"start", "grp.x", "grp.y", "end"}
    assert set(dag.intra) == {("grp.x", "grp.y"), ("start", "grp.x"), ("grp.y", "end")}


def test_dag_id_und_schedule_und_periode() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="0 */6 * * *") as dag:
            EmptyOperator(task_id="a")
        """
    )
    assert dag.schedule_expr == "'0 */6 * * *'"
    assert dag.period_s == 6 * 3600.0
    timed = parse_one(
        """
        with DAG(dag_id="t", schedule=timedelta(hours=4)) as dag:
            EmptyOperator(task_id="a")
        """
    )
    assert timed.period_s == 4 * 3600.0


def test_dag_id_nicht_statisch_bleibt_none() -> None:
    dag = parse_one(
        """
        def name():
            return "x"

        with DAG(dag_id=name(), schedule="@hourly") as dag:
            EmptyOperator(task_id="a")
        """
    )
    assert dag.dag_id is None


# --- Die drei Nicht-aufloesbar-Faelle ------------------------------------------------


def test_fstring_task_id_wird_platzhalter_und_kanten_verfallen() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            start = EmptyOperator(task_id="start")
            for i in range(3):
                t = BashOperator(task_id=f"load_{i}", bash_command="x")
                start >> t
        """
    )
    assert "load_{i}?" in dag.tasks
    assert "dynamic_task_id" in warning_kinds(dag)
    assert "edge_dropped" in warning_kinds(dag)
    assert dag.intra == ()


def test_expand_ist_eine_task_mit_warnung() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            BashOperator.partial(task_id="mapped", bash_command="x").expand(env=[{}, {}])
        """
    )
    assert "mapped" in dag.tasks
    assert "task_mapping" in warning_kinds(dag)


def test_dag_variable_aus_fremdem_modul_ein_dag_im_file_wird_zugeordnet() -> None:
    dag = parse_one(
        """
        from somewhere import fremd

        with DAG(dag_id="d", schedule="@hourly") as dag:
            EmptyOperator(task_id="a")

        BashOperator(task_id="b", bash_command="x", dag=fremd)
        """
    )
    assert set(dag.tasks) == {"a", "b"}
    assert "task_dag_inferred" in warning_kinds(dag)


def test_dag_variable_aus_fremdem_modul_mehrere_dags_keine_zuordnung() -> None:
    result = parse_source(
        HEADER
        + dedent(
            """
            from somewhere import fremd

            with DAG(dag_id="d1", schedule="@hourly") as one:
                EmptyOperator(task_id="a")

            with DAG(dag_id="d2", schedule="@hourly") as two:
                EmptyOperator(task_id="b")

            BashOperator(task_id="waise", bash_command="x", dag=fremd)
            """
        ),
        "dags/x.py",
    )
    all_tasks = {t for d in result.dags for t in d.tasks}
    assert "waise" not in all_tasks
    assert "ambiguous_task" in {w.kind for w in result.warnings}


# --- Import-Check: DAG muss aus airflow kommen ---------------------------------------


def test_lokale_dag_klasse_wird_nicht_geparst() -> None:
    source = dedent(
        """
        class DAG:
            def __init__(self, **kwargs):
                pass

        d = DAG(dag_id="fake", schedule="@hourly")
        """
    )
    result = parse_source(source, "client/models.py")
    assert result.dags == ()
    assert "dag_not_airflow" in {w.kind for w in result.warnings}


def test_dag_aus_fremdem_modul_wird_nicht_geparst() -> None:
    source = dedent(
        """
        from openapi_client.models import DAG

        d = DAG(dag_id="fake", schedule="@hourly")
        """
    )
    result = parse_source(source, "client/api.py")
    assert result.dags == ()
    assert "dag_not_airflow" in {w.kind for w in result.warnings}


def test_dag_ohne_import_beleg_wird_nicht_geparst() -> None:
    source = 'd = DAG(dag_id="unbelegt", schedule="@hourly")\n'
    result = parse_source(source, "dags/x.py")
    assert result.dags == ()
    assert "dag_not_airflow" in {w.kind for w in result.warnings}


def test_dag_aus_airflow_models_wird_geparst() -> None:
    source = dedent(
        """
        from airflow.models import DAG

        with DAG(dag_id="ok", schedule="@hourly") as dag:
            pass
        """
    )
    result = parse_source(source, "dags/x.py")
    assert len(result.dags) == 1


def test_fremde_files_loesen_keine_python_syntax_warnungen_aus() -> None:
    # Korpus-Fund (Verifikation 009): "\;" in einem bash_command laesst ast.parse eine
    # SyntaxWarning auf stderr schreiben. Fremde Files sind Systemgrenze — der Befund
    # gehoert ins Warning_-Modell, nicht in den Terminal-Output der CLI.
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        parse_source('x = "\\;"\n', "dags/fremd.py")
    assert [w for w in caught if issubclass(w.category, SyntaxWarning)] == []


def test_syntaxfehler_ist_warnung_nicht_absturz() -> None:
    result = parse_source('print "python2"\n', "dags/broken.py")
    assert result.dags == ()
    assert "syntax_error" in {w.kind for w in result.warnings}


# --- Uebersetzungstabelle: A ---------------------------------------------------------


def test_a_depends_on_past_am_operator_ist_selbstkante() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = BashOperator(task_id="b", bash_command="x", depends_on_past=True)
            a >> b
        """
    )
    assert edges(dag) == {("b", "b", 1, "depends_on_past")}


def test_a_in_default_args_gilt_fuer_jede_task_operator_ebene_ueberschreibt() -> None:
    dag = parse_one(
        """
        with DAG(
            dag_id="d",
            schedule="@hourly",
            default_args={"depends_on_past": True},
        ) as dag:
            one = EmptyOperator(task_id="one")
            two = BashOperator(task_id="two", bash_command="x", depends_on_past=False)
        """
    )
    assert edges(dag) == {("one", "one", 1, "depends_on_past")}


def test_a_depends_on_past_false_ist_keine_kante() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            BashOperator(task_id="a", bash_command="x", depends_on_past=False)
        """
    )
    assert dag.cross == ()


# --- Uebersetzungstabelle: B ---------------------------------------------------------


def test_b_wait_for_downstream_selbstkante_plus_direkte_downstreams() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = BashOperator(task_id="b", bash_command="x", wait_for_downstream=True)
            c = EmptyOperator(task_id="c")
            d = EmptyOperator(task_id="d")
            a >> b >> c >> d
        """
    )
    # b impliziert depends_on_past (Selbstkante) und wartet auf die *direkten*
    # Downstreams des Vorlaufs: c ja, d nein (nur direkte, so ist Airflow definiert).
    assert edges(dag) == {
        ("b", "b", 1, "wait_for_downstream"),
        ("c", "b", 1, "wait_for_downstream"),
    }


def test_b_und_a_beide_gesetzt_beide_signale_sichtbar() -> None:
    # Korpus-Fund (scotthavens/docker-airflow u.a.): beide Flags in default_args.
    # B impliziert A, aber erkannt sind beide Signale — die Selbstkante traegt A,
    # die Downstream-Kanten tragen B, sonst weicht der Parser vom Scanner ab.
    dag = parse_one(
        """
        with DAG(
            dag_id="d",
            schedule="@hourly",
            default_args={"depends_on_past": True, "wait_for_downstream": True},
        ) as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            a >> b
        """
    )
    signals = {e.signal for e in dag.cross}
    assert signals == {"depends_on_past", "wait_for_downstream"}
    assert ("a", "a", 1, "depends_on_past") in edges(dag)
    assert ("b", "a", 1, "wait_for_downstream") in edges(dag)


def test_tasks_in_klassen_methoden_werden_gefunden() -> None:
    # Korpus-Fund (Airflows eigene Testfiles): DAG und Sensor leben im Rumpf einer
    # unittest-Methode. Der Walker muss in ClassDef-Ruempfe absteigen.
    dag = parse_one(
        """
        class TestSensor:
            def setUp(self):
                dag = DAG(dag_id="t", schedule="@hourly")
                wait = ExternalTaskSensor(
                    task_id="wait",
                    external_dag_id="other",
                    execution_delta=timedelta(hours=1),
                    dag=dag,
                )
        """
    )
    assert "wait" in dag.tasks
    assert "sensor_not_modeled" in warning_kinds(dag)


# --- Uebersetzungstabelle: C ---------------------------------------------------------

SENSOR_PAIR = """
with DAG(dag_id="up", schedule="{up_schedule}") as up:
    done = EmptyOperator(task_id="done")

with DAG(dag_id="down", schedule="@hourly") as down:
    wait = ExternalTaskSensor(
        task_id="wait",
        external_dag_id="{external}",
        external_task_id="done",
        {offset}
    )
"""


def sensor_pair(offset: str, external: str = "up", up_schedule: str = "@hourly") -> ParsedDag:
    dags = parse(SENSOR_PAIR.format(offset=offset, external=external, up_schedule=up_schedule))
    assert len(dags) == 2
    return dags[1]


def test_c_execution_delta_ganzzahliges_vielfaches_wird_kante_mit_periods() -> None:
    down = sensor_pair("execution_delta=timedelta(hours=2),")
    assert edges(down) == {("up.done", "wait", 2, "external_task_sensor")}


def test_c_ziel_nicht_im_parse_satz_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("execution_delta=timedelta(hours=1),", external="anderswo")
    assert down.cross == ()
    assert "sensor_not_modeled" in warning_kinds(down)


def test_c_verschiedene_takte_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("execution_delta=timedelta(hours=24),", up_schedule="@daily")
    assert down.cross == ()
    assert "sensor_not_modeled" in warning_kinds(down)


def test_c_nicht_ganzzahliges_verhaeltnis_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("execution_delta=timedelta(minutes=90),")
    assert down.cross == ()
    assert "sensor_not_modeled" in warning_kinds(down)


def test_c_nicht_aufloesbarer_versatz_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("execution_delta=OFFSET,")
    assert down.cross == ()
    assert "sensor_not_modeled" in warning_kinds(down)


def test_c_execution_date_fn_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("execution_date_fn=lambda d: d,")
    assert down.cross == ()
    assert "sensor_dynamic_offset" in warning_kinds(down)


def test_c_versatz_null_ist_intra_run_und_kein_signal() -> None:
    down = sensor_pair("execution_delta=timedelta(hours=0),")
    assert down.cross == ()
    assert warning_kinds(down) == set()


# --- Uebersetzungstabelle: C, Selbst-Referenz (ADR-021) -------------------------------

SELF_SENSOR = """
with DAG(dag_id="selbst", schedule="@hourly") as dag:
    done = EmptyOperator(task_id="done")
    wait = ExternalTaskSensor(
        task_id="wait",
        external_dag_id="selbst",
        external_task_id="done",
        {offset}
    )
    wait >> done
"""


def self_sensor(offset: str) -> ParsedDag:
    return parse_one(SELF_SENSOR.format(offset=offset))


def test_c_selbst_referenz_mit_einer_periode_wird_kante() -> None:
    dag = self_sensor("execution_delta=timedelta(hours=1),")
    assert edges(dag) == {("selbst.done", "wait", 1, "external_task_sensor")}
    assert "sensor_not_modeled" not in warning_kinds(dag)


def test_c_selbst_referenz_mit_zwei_perioden_wird_kante_mit_periods_2() -> None:
    dag = self_sensor("execution_delta=timedelta(hours=2),")
    assert edges(dag) == {("selbst.done", "wait", 2, "external_task_sensor")}


def test_c_selbst_referenz_kein_vielfaches_keine_kante_sondern_warnung() -> None:
    dag = self_sensor("execution_delta=timedelta(minutes=90),")
    assert dag.cross == ()
    assert "sensor_not_modeled" in warning_kinds(dag)


def test_c_selbst_referenz_kante_landet_im_eigenen_namespace() -> None:
    dag = self_sensor("execution_delta=timedelta(hours=1),")
    pipeline = to_pipeline([dag])
    assert ("selbst.done", "selbst.wait") not in pipeline.intra
    assert [(e.src, e.dst, e.periods) for e in pipeline.cross] == [
        ("selbst.done", "selbst.wait", 1)
    ]


# --- Uebersetzungstabelle: D und F ---------------------------------------------------


def test_d_include_prior_dates_keine_kante_sondern_warnung() -> None:
    down = sensor_pair("include_prior_dates=True,")
    assert down.cross == ()
    assert "include_prior_dates" in warning_kinds(down)


def test_f_prev_success_template_ist_befund_ohne_kante() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            BashOperator(
                task_id="load",
                bash_command="load --since {{ prev_start_date_success }}",
            )
        """
    )
    # ADR-020: das Template rendert einen Zeitstempel und wartet nicht.
    # Marktzahl ja, Lambda-Kante nein.
    assert dag.cross == ()
    assert "prev_run_success" in warning_kinds(dag)


def test_f_schwache_variante_wird_getrennt_gemeldet() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            BashOperator(task_id="label", bash_command="load --day {{ prev_ds }}")
        """
    )
    assert dag.cross == ()
    assert "prev_run_date" in warning_kinds(dag)


# --- Uebersetzungstabelle: G ---------------------------------------------------------


def test_g_max_active_runs_1_verbindet_jede_senke_mit_jeder_quelle() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly", max_active_runs=1) as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            a >> b
            a >> c
        """
    )
    assert edges(dag) == {
        ("b", "a", 1, "max_active_runs"),
        ("c", "a", 1, "max_active_runs"),
    }


def test_g_groesser_1_oder_dynamisch_ist_keine_kante() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly", max_active_runs=3) as dag:
            EmptyOperator(task_id="a")
        """
    )
    assert dag.cross == ()


# --- to_pipeline ---------------------------------------------------------------------


def test_to_pipeline_selbstkante_ergibt_lambda_kleiner_critical_path() -> None:
    from eigenlag.maxplus import condense, critical_path, karp

    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
            b = BashOperator(task_id="b", bash_command="x", depends_on_past=True)
            c = EmptyOperator(task_id="c")
            a >> b >> c
        """
    )
    pipeline = to_pipeline([dag])
    graph, _ = condense(pipeline)
    # Der Kreis ist ein Teilpfad: nur b liegt darauf. Uniforme Dauer 1.0 je Task,
    # also Lambda = 1.0, waehrend der Critical Path drei Tasks lang ist.
    assert karp(graph) == 1.0
    assert critical_path(pipeline)[0] == 3.0


def test_to_pipeline_g_kante_macht_lambda_zum_makespan() -> None:
    from eigenlag.maxplus import condense, karp

    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly", max_active_runs=1) as dag:
            a = EmptyOperator(task_id="a")
            b = EmptyOperator(task_id="b")
            c = EmptyOperator(task_id="c")
            a >> b >> c
        """
    )
    graph, _ = condense(to_pipeline([dag]))
    # ADR-019: Lauf k startet erst nach Lauf k-1, Lambda ist der Makespan.
    assert karp(graph) == 3.0


def test_to_pipeline_sensor_versatz_halbiert_das_zyklusmittel() -> None:
    from eigenlag.maxplus import condense, karp

    dags = parse(
        """
        with DAG(dag_id="up", schedule="@hourly") as up:
            done = BashOperator(task_id="done", bash_command="x", depends_on_past=True)

        with DAG(dag_id="down", schedule="@hourly") as down:
            wait = ExternalTaskSensor(
                task_id="wait",
                external_dag_id="up",
                external_task_id="done",
                execution_delta=timedelta(hours=2),
            )
        """
    )
    pipeline = to_pipeline(dags)
    assert "up.done" in pipeline.durations
    assert "down.wait" in pipeline.durations
    graph, _ = condense(pipeline)
    # Zwei Kreise: die Selbstkante an up.done (1.0 / 1) und keiner ueber den Sensor
    # (die Sensor-Kante hat keinen Rueckweg). Lambda bleibt 1.0; die Sensor-Kante
    # existiert mit periods=2 in der Pipeline.
    assert karp(graph) == 1.0
    assert any(e.periods == 2 for e in pipeline.cross)


def test_to_pipeline_uniforme_dauer_ist_default_eigene_dauern_moeglich() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            a = EmptyOperator(task_id="a")
        """
    )
    assert to_pipeline([dag]).durations == {"d.a": 1.0}
    assert to_pipeline([dag], durations={"d.a": 2.5}).durations == {"d.a": 2.5}


def test_parsed_cross_edge_traegt_herkunft() -> None:
    dag = parse_one(
        """
        with DAG(dag_id="d", schedule="@hourly") as dag:
            BashOperator(task_id="b", bash_command="x", depends_on_past=True)
        """
    )
    edge = dag.cross[0]
    assert isinstance(edge, ParsedCrossEdge)
    assert edge.file == "dags/x.py"
    assert edge.lineno > 0
