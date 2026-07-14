# Stichproben zum Re-Scan (Session 006)

Drei Stichproben, wie in Spec 006 Abschnitt 4 verlangt, dazu die Ursachen-Zuordnung des
Vorher/Nachher-Deltas aus Abschnitt 3. Gezogen mit `random.Random(6)` aus
`scan/v2/scan_results.csv`, je ein DAG pro Repo, damit kein Repo die Stichprobe dominiert.
Geprüft wurde gegen den Quelltext im Clone unter `data/repos/`; der Commit-SHA jedes Belegs
steht im Permalink der jeweiligen CSV-Zeile.

## 1. Kern-Kandidaten: 10 von 176 (aus 100 Repos)

Prüfkriterien je Zeile: Steht das Signal wirklich an der genannten Zeile? Gehört es zu diesem
DAG? Ist der Schedule wirklich sub-täglich?

| # | Repo | DAG | Schedule | Beleg (Datei:Zeile) | Tatsächlicher Inhalt | Urteil |
|---|---|---|---|---|---|---|
| 1 | `mylons/superpipe` | `bake_off` | `timedelta(minutes=10)` | `dags/bakeoff.py:77` | `wait_for_downstream=True)` am Operator, gebunden über `dag=BAKE_OFF_PIPE` | echt |
| 2 | `DmitriiDenisov/airflow-lab` | `branch_list_ex` | `timedelta(minutes=2)` | `dags_examples/branch_list_ex.py:33` | `'depends_on_past': True,` in `default_args` | echt |
| 3 | `jiyooonkim/Coding` | `aggregate_data` | `* 7 * * *` (feuert minütlich in Stunde 7, kleinste Distanz 60 s) | `python/airflow.py:32`, `:70` | `'depends_on_past': True,` und `execution_delta=timedelta(hours=3, minutes=30)` | echt |
| 4 | `wbe7/dag` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/example_dags/example_branch_python_dop_operator_3.py:31` | `'depends_on_past': True,` | echt |
| 5 | `airflow198612/airflow2` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/dist-packages/airflow/example_dags/example_branch_python_dop_operator_3.py:25` | `'depends_on_past': True,` | echt |
| 6 | `BigDataMatrix/DataPipeline` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/example_dags/example_branch_python_dop_operator_3.py:28` | `'depends_on_past': True,` | echt |
| 7 | `8ubble8uddy/de-project-sprint-5` | `settlements_mart` | `0/15 * * * *` | `src/dags/sprint5_dag.py:48`, `:53`, `:58`, dazu `:39` | dreimal `prev_start_date_success` als Callable-Parameter (ADR-013), dazu `max_active_runs=1` als G-Spalte | echt |
| 8 | `Prashant15887/apache-airflow-2.10.4` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/example_dags/example_branch_python_dop_operator_3.py:54` | `default_args={"depends_on_past": True},` | echt |
| 9 | `sandys/tf-ecs-airflow` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/airflow/example_dags/example_branch_python_dop_operator_3.py:52` | `default_args={'depends_on_past': True},` | echt |
| 10 | `onestn/airflow-2.10.4` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/example_dags/example_branch_python_dop_operator_3.py:54` | `default_args={"depends_on_past": True},` | echt |

**Ergebnis: 0 Falsch-Positive von 10.** Die Demo-Lastigkeit ist unverändert sichtbar (sieben
der zehn sind Kopien von Airflows `example_branch_dop_operator_v3`); der Report beziffert sie.

## 2. G-only-Kandidaten: 10 von 473 (aus 159 Repos)

Prüfkriterien: Steht `max_active_runs=1` explizit als Literal (kein auflösbarer Ausdruck, kein
Default)? Ist der Schedule sub-täglich?

| # | Repo | DAG | Schedule | Beleg (Datei:Zeile) | Tatsächlicher Inhalt | Urteil |
|---|---|---|---|---|---|---|
| 1 | `nagazo/airflow_dag` | `agg` | `*/30 * * * *` | `dags/agg.py:28` | `max_active_runs=1,` | echt |
| 2 | `gvm2121/airflow_dags` | `test_dummy_dag` | `*/2 * * * *` | `airflow/providers/openlineage/tests/unit/openlineage/extractors/test_bash.py:38` | `max_active_runs=1,` | echt |
| 3 | `dipampaul17/protean` | `listing_4_15` | `@hourly` | `data/raw/oss_dags/BasPH_data-pipelines-with-apache-airflow_chapter04_dags_listing_4_15.py:12` | `max_active_runs=1,` | echt |
| 4 | `Bensonn5151/crypto-app` | `coindesk_data_pipeline` | `*/1 * * * *` | `airflow-docker/dags/coindesk_dag.py:141` | `max_active_runs=1,` | echt |
| 5 | `beefjerky/new_airflow` | ohne `dag_id` (Variable, geflaggt) | `* * * * *` | `tests/jobs.py:1108` | `max_active_runs=1,` | echt |
| 6 | `njuxc/PYAM` | ohne `dag_id` (Variable, geflaggt) | `* * * * *` | `Dataset/commit_files/airflow/75f25bd8b96841689d1d9854a738db91c302bb63/tests#jobs#test_scheduler_job.py:2889` | `max_active_runs=1,` im `DAG(...)`-Aufruf | echt |
| 7 | `Tristan-Chow/airflow` | ohne `dag_id` (Variable, geflaggt) | `* * * * *` | `airflow-1.10.12/tests/jobs/test_scheduler_job.py:2602` | `max_active_runs=1,` | echt |
| 8 | `khairulhabibatibm/airflow-py38` | ohne `dag_id` (f-String, geflaggt) | `timedelta(minutes=5)` | `airflow/smart_sensor_dags/smart_sensor_group.py:44` | `max_active_runs=1,` | echt |
| 9 | `sergeykuznetsov1995/data-platform-football` | `superset_alerts` | `*/15 * * * *` | `dags/dag_superset_alerts.py:241` | `max_active_runs=1,` | echt |
| 10 | `shaheerbeig/Ecommerce-Event-Driven-Architecture-ETL` | `bronze_ingestion` | `*/30 * * * *` | `airflow/dags/bronze_ingestion_dag.py:28` | `max_active_runs=1,` | echt |

**Ergebnis: 0 Falsch-Positive von 10.** Jedes `max_active_runs=1` ist ein Integer-Literal am
DAG-Aufruf, kein Default und kein Ausdruck. Kein einziger dieser DAGs trägt ein A–F-Signal, die
Klassen-Trennung (ADR-018) greift also.

**Nebenbefund mit Fix:** Zeile 6 (`njuxc/PYAM`) liegt in einer Datei mit `#` im Namen
(`tests#jobs#test_scheduler_job.py`). Der Permalink war dadurch nicht nachschlagbar, weil das
`#` den Zeilen-Anker abschnitt (Verstoß gegen Regel 6). `permalink()` in `scanner/report.py`
encodiert Pfade seit dieser Session (`%23`), Test `test_permalink_encodes_hash_and_space_in_the_path`.

## 3. Falsch-Negative: 10 von 712 Repos mit DAGs, aber ohne jedes Signal

Frage je Repo: Gibt es einen legitimen Grund für "kein Signal", oder kennt der Scanner ein
Muster nicht? Geprüft per Volltextsuche über alle Signal-Schlüsselwörter im Clone
(`depends_on_past`, `wait_for_downstream`, `ExternalTaskSensor`, `include_prior_dates`,
`prev_*`, `max_active_runs`, `is_incremental`), jeden Treffer von Hand eingeordnet.

| # | Repo | Fundstellen der Suchbegriffe | Grund für "kein Signal" | Urteil |
|---|---|---|---|---|
| 1 | `youthtoday/EECS-6893-HW` | `HW4/task2-1.py:51` u. a. | überall `'depends_on_past': False`, einmal `# 'wait_for_downstream': False` als Kommentar | legitim |
| 2 | `Hamza-br/Climate_and_Sentiment_Tracker` | `dags/ml_training_dag.py:7`, `dags/spark_processing_dag.py:7` | `'depends_on_past': False` | legitim |
| 3 | `Romesh-Thok/weather-data-analysis` | `dags/transform_redshift_load.py:3`, `:8` | `ExternalTaskSensor` nur importiert, nie instanziiert; dazu `'depends_on_past': False` | legitim |
| 4 | `pf6511/alpr-mlops-platform` | `airflow/dags/retrain_efficientnet.py:31` | `'depends_on_past': False` | legitim |
| 5 | `patil-pus/Retail-BigData-Pipeline` | `airflow/dags/process_with_spark_dag.py:7` | `'depends_on_past': False` | legitim |
| 6 | `armanraj02/MILESTONE_2` | `dags/spotify_pipeline.py:11` | `'depends_on_past': False` | legitim |
| 7 | `vamegah/data-engineering` | sieben Pipelines, z. B. `hr/dags/hr_pipeline.py:11` | überall `"depends_on_past": False` | legitim |
| 8 | `shaqbari/de6_3th_day6_naver` | `dags/dm_dag.py:278`, `dags/dw_dag.py:123` | `ExternalTaskSensor` mit `execution_delta=timedelta(hours=0)`: Null-Versatz zeigt auf denselben Logical Date, Intra-Run-Kante, genau der ADR-014-Fall | legitim |
| 9 | `zachliu/airflow-python-sdk` | `airflow_python_sdk/model/task.py:102` u. a. | generierter API-Client: die Wörter stehen als Dict-Schlüssel und Doku-Strings in Modell-Definitionen, kein Operator-Aufruf | legitim |
| 10 | `GitSmide/de-project-4` | `src/dags/refresh_cdm.py:19` u. a. | `'depends_on_past': False` | legitim |

**Ergebnis: 0 Falsch-Negative von 10, kein unbekanntes Muster.** Der wertvollste Fund ist
Zeile 8: der explizite Null-Versatz, den ADR-014 aus der 003-Stichprobe heraus zum
Nicht-Signal erklärt hat, kommt in freier Wildbahn erneut vor und wird korrekt übergangen.

## 4. Vorher/Nachher: jedes Delta einer Ursache zugeordnet

| Größe | 003 | 006 | Delta | Ursache |
|---|---|---|---|---|
| DAGs gefunden | 51.426 | 51.789 | +363 | ADR-015: +422 Konstruktor-Aufrufstellen, −59 Schablonen (unten belegt) |
| DAGs mit Cross-Run-Kante (A–F) | 1.303 | 1.303 | 0 | Treffer-**Menge** identisch, nicht nur die Anzahl (per Key `repo, file, dag_id, lineno` verglichen) |
| Risiko-Kandidaten (Kern) | 176 | 176 | 0 | dieselben 176 Zeilen wie in 003, Definition und Menge unverändert |
| Risiko-Kandidaten (nur G) | — | 473 | +473 | neue Klasse (ADR-016/018), in 003 nicht erhoben |
| DAGs ohne `dag_id` | 4.587 | 4.952 | +365 | 401 der 422 neuen Konstruktor-Zeilen haben keine `dag_id`; erst der Aufrufer setzt sie |
| dbt-Models mit Selbst-Kante | 3.369 | 3.369 | 0 | `analyze_dbt.py` unverändert, `scan_dbt.csv` byte-identisch übernommen (per `diff` geprüft) |

Die Kontrolle in die Gegenrichtung: das neue `report.py`, auf den **alten** 003-State
angewandt, reproduziert exakt 51.426 / 1.303 / 176 mit 0 G-only. Die Kern-Quote ist damit in
beide Richtungen definitionsgleich belegt.

### Die Spec-Stichprobe zum ADR-015-Delta, und warum sie anders ausfällt als erwartet

Spec 006 verlangt 5 neue Kern-Kandidaten mit Konstruktor-Nachweis. **Es gibt keine:** die
Kern-Menge ist identisch mit 003, es ist nichts zuzuordnen. Die Erwartung aus STATUS 005
("die Marktzahl steigt deutlich") ist damit gemessen widerlegt. Der öffentliche Korpus kapselt
seine DAG-Erzeugung kaum; Konstruktoren wie bei Wikimedia sind ein Muster professioneller
Umgebungen, und genau die sind in der Code-Search-Stichprobe unterrepräsentiert. Das ist die
ehrlichere Pointe von ADR-015: er ändert an der öffentlichen Marktzahl fast nichts, an
Wikimedia hat er die DAG-Zahl verfünffacht.

An seine Stelle treten zwei 5er-Stichproben über das Delta, das es wirklich gibt:

**5 der 422 neuen DAG-Zeilen** (alle signalfrei, daher ohne Wirkung auf Zähler und Quote):

| Repo | Aufrufstelle | Konstruktor |
|---|---|---|
| `EKS-APP/apache-airflow` | `tests/models/test_dagrun.py:649` | `with_all_tasks_removed(dag)`; definiert in `:638` als `return DAG(dag_id=..., start_date=...)` |
| `mik-laj/airflow-api-clients` | `out/oneoff/python/test/test_dag_state.py:47` | `self.make_instance(...)`; in `out/string/python/test/test_dag.py:32` gibt `make_instance` das API-Modell `DAG(...)` zurück |
| `mik-laj/airflow-api-clients` | `out/oneoff/python/test/test_relative_delta.py:65` | wie oben |
| `mik-laj/airflow-api-clients` | `out/oneoff/python/test/test_variable_all_of.py:48` | wie oben |
| `mik-laj/airflow-api-clients` | `out/object/python/test/test_import_error.py:51` | wie oben |

330 der 422 neuen Zeilen stammen allein aus `mik-laj/airflow-api-clients`: ein generierter
OpenAPI-Client, dessen Modellklasse zufällig `DAG` heißt und dessen Test-Methode
`make_instance` sie zurückgibt. Weil ADR-015 Konstruktor-**Namen** repo-weit auflöst (der
Konstruktor steht in einem anderen File als seine Aufrufe), zählt danach jedes
`self.make_instance(...)` im Repo als DAG-Scope, auch in Tests fremder Modelle. Das bläht den
Nenner dieses einen Repos auf, trägt kein einziges Signal und berührt keine Quote. Es ist der
dokumentierte Preis der bewusst schlichten Erkennungsregel, kein neues Signal-Muster; eine
import-genaue Auflösung wäre ein ADR-Kandidat für eine spätere Session, keine Regel-Änderung
in dieser (Spec 006, "Explizit nicht in dieser Session").

**5 der 59 weggefallenen Zeilen** (erwartet: die Schablone im Konstruktor-Rumpf, die 003 als
DAG zählte und die nach ADR-015 nicht mehr zählt, weil sonst ein Repo einen DAG mehr hätte als
Aufrufstellen):

| Repo | Zeile | Befund |
|---|---|---|
| `MatrixManAtYrService/airflow-challenge` | `airflow/models/dag.py:2995` | `with DAG(*dag_bound_args.args, ...)` im Rumpf von Airflows eigenem `@dag`-Dekorator (vendorierter Airflow-Quellcode) |
| `gvm2121/airflow_dags` | `airflow/airflow-core/tests/unit/dags/test_multiple_dags.py:29` | `dag = DAG(...)` im Rumpf von `create_dag(suffix)`, das den DAG zurückgibt |
| `makerdao-data/blank-airflow` | `airflow/models/dag.py:2970` | wie Zeile 1, vendorierter `@dag`-Dekorator |
| `meyerjo2024/airflow1` | `airflow-core/tests/unit/serialization/test_dag_serialization.py:741` | `with DAG(...)` im Rumpf von `create_dag()` |
| `uyoungii/airflow-2.1.4` | `airflow-2.1.4/tests/www/views/test_views_graph_gantt.py:42` | `dag = DAG(DAG_ID, ...)` im Rumpf einer Fixture, die den DAG zurückgibt |

Alle fünf sind Funktions-Rümpfe, die ein `DAG(...)` zurückgeben, also Schablonen im Sinn von
ADR-015. Keiner trug in 003 ein Signal; die 59 Wegfälle berühren weder Cross-Run-Zahl noch
Quote.
