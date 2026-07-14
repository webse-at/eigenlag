# Stichproben zum Scan-Lauf (Session 003)

Zwei Stichproben, wie in Spec 003 Abschnitt 8 verlangt: eine gegen Falsch-Positive, eine gegen
Falsch-Negative. Beide auf dem **finalen** Lauf (Lauf 3, nach der Korrektur des Nullversatzes,
ADR-014). Gezogen mit `random.Random(3)`, je ein DAG pro Repo, damit kein Repo die Stichprobe
dominiert. Geprüft wurde gegen den Quelltext im Clone, dessen Commit-SHA im Permalink steht.

## 8a. Falsch-Positive: 10 von 176 Risiko-Kandidaten

Prüfkriterien je Zeile: Steht das Signal wirklich an der genannten Zeile? Gehört es zu diesem
DAG? Ist der Schedule wirklich sub-täglich?

| # | Repo | DAG | Schedule | Beleg (Datei:Zeile) | Tatsächlicher Inhalt | Urteil |
|---|---|---|---|---|---|---|
| 1 | `aditishankar/test` | `test_dag_v1` | `*/10 * * * *` | `docs/docker-stack/docker-examples/extending/embedding-dags/test_dag.py:31` | `default_args = {'owner': 'airflow', 'depends_on_past': True, ...}` | echt |
| 2 | `onestn/airflow-2.10.4` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `airflow/example_dags/example_branch_python_dop_operator_3.py:54` | `default_args={"depends_on_past": True},` | echt |
| 3 | `makerdao-data/blank-airflow` | `test_dag_v1` | `*/10 * * * *` | `docs/docker-stack/docker-examples/extending/embedding-dags/test_dag.py:35` | `default_args={'depends_on_past': True},` | echt |
| 4 | `MrE-Fog/airflow` | `test_dag_v1` | `*/10 * * * *` | `docs/docker-stack/docker-examples/extending/embedding-dags/test_dag.py:37` | `default_args={'depends_on_past': True},` | echt |
| 5 | `davilayang/compose-airlfow` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `backup_dags/example_dags/example_branch_python_dop_operator_3.py:14` | `'depends_on_past': True,  # arguments for the branching operator` | echt |
| 6 | `oxylabs/building-scraping-pipeline-apache-airflow` | `scrape` | `* * * * *` | `DAG/scrape.py:10`, `:26`, `:33` | `'depends_on_past': True,` sowie zweimal `python_callable=lambda prev_start_date_success: ...` | echt, davon zwei Treffer erst durch ADR-013 |
| 7 | `jcw024/lichess_database_ETL` | `lichess_ETL_pipeline_local_processing` | `timedelta(minutes=5)` | `src/airflow_dag_local.py:39` | `'depends_on_past':True,` | echt |
| 8 | `plivo/airflow` | `test_dag_v1` | `*/10 * * * *` | `dags/test_dag.py:26` | `'depends_on_past': True,` | echt |
| 9 | `no-brand/airflow-fork` | `test_dag_v1` | `*/10 * * * *` | `docs/docker-stack/docker-examples/extending/embedding-dags/test_dag.py:35` | `default_args={'depends_on_past': True},` | echt |
| 10 | `DSDE-Project/job-portal` | `example_branch_dop_operator_v3` | `*/1 * * * *` | `example_dags/example_branch_python_dop_operator_3.py:54` | `default_args={"depends_on_past": True},` | echt |

**Ergebnis: 0 Falsch-Positive von 10.** Jeder Permalink löst auf die genannte Zeile auf, jedes
Signal gehört zum ausgewiesenen DAG, jeder Schedule ist sub-täglich.

**Der inhaltliche Befund der Stichprobe ist ein anderer und wiegt schwerer:** acht der zehn
Treffer sind Beispiel-, Test- oder Doku-Code, in aller Regel eine Kopie von Airflows eigenem
`example_branch_dop_operator_v3` (trägt `depends_on_past=True` bei `*/1 * * * *`) oder von
`test_dag.py` aus der Docker-Doku (`depends_on_past=True` bei `*/10 * * * *`). Der Scanner
arbeitet also korrekt, aber die Grundgesamtheit trägt die Aussage nicht. Der Report beziffert
das: 138 der 176 Risiko-Kandidaten liegen in einem Demo-Pfad.

### Der Falsch-Positive aus Lauf 2, der zur Korrektur führte

Die Stichprobe auf Lauf 2 enthielt `Dat-Al/Fidai`, `airflow/dags/predict_hourly_dag.py:37`:

```python
execution_delta=timedelta(hours=0),  # Regarde la même heure d'exécution
```

Ein `execution_delta` von null zeigt auf denselben Logical Date. Das ist eine Intra-Run-Kante
zwischen zwei DAGs und **kein** Cross-Run-Signal (`wiki/signals.md`, Signal C). Der Scanner
zählte jedes gesetzte `execution_delta`, das nicht `None` war. Korrigiert in ADR-014, Lauf
wiederholt. Wirkung: Cross-Run-DAGs 1335 → 1303, Risiko-Kandidaten 182 → 176.

## 8b. Falsch-Negative: 10 von 964 signalfreien Airflow-Kandidaten

Gezogen aus den Repos, die die Code-Search als Kandidat geliefert hat, in denen der Scanner
aber kein Signal meldet. Frage: Gibt es einen legitimen Grund, oder kennt der Scanner ein
Muster nicht?

| # | Repo | Fundstelle des Suchbegriffs | Grund für "kein Signal" | Urteil |
|---|---|---|---|---|
| 1 | `ParthSoni-CS/ecommerce-realtime-datastreaming-processing` | `dags/logs_producer.py:97` | `'depends_on_past': False` | legitim |
| 2 | `jkacosta91/CursoDataEngineeringCoderHouse` | `dags/dag_anime.py:20` | `'depends_on_past': False` | legitim |
| 3 | `gziz/etl-airflow-ce` | `etl_dags.py:10` | `'depends_on_past': False` | legitim |
| 4 | `Guimarret/sw_data_filtering` | `dags/sw_pipeline.py:8` | `'depends_on_past': False` | legitim |
| 5 | `artchernenko/airflow` | `dags/tutorial_new.py:23` | `# 'wait_for_downstream': False,` — auskommentiert | legitim |
| 6 | `workforce-data-initiative/skills-airflow` | `utils/dags.py:13`, `dags/open_skills_master.py:22` | `'depends_on_past': False` | legitim |
| 7 | `juicebocks27/movie-recommendation-engine` | `airflow/dags/generate_pickles.py:19` | `'depends_on_past': False` | legitim |
| 8 | `dinod001/Fraud-Detection-System` | `pipeline/.airflow/dags/inference_pipeline_dag.py:24` | `'depends_on_past' : False` | legitim |
| 9 | `kimjaejeong/MLOps-API` | `airflow_test/dags/tutorial.py:23` | `# 'wait_for_downstream': False,` — auskommentiert | legitim |
| 10 | `jataware/k8s_to_S3` | `airflow/dags/fsc.py:34`, `:64` | `'depends_on_past': False`, `wait_for_downstream=False` | legitim |

**Ergebnis: 0 unbekannte Muster von 10.** Alle zehn schweigen aus einem Grund, den die
Signal-Definition ausdrücklich vorsieht: explizite Verneinung oder auskommentierter Code. Genau
hier zahlt sich der AST aus, denn ein Regex hätte in allen zehn Fällen einen Treffer gemeldet.

### Die zwei Muster, die außerhalb der Stichprobe gefunden wurden

Die Stichprobe war sauber, eine gezielte Nachsuche war es nicht. Zwei Zahlen im Zwischenreport
sahen zu klein aus, um sie ungeprüft stehen zu lassen, und die Prüfung hat zwei echte
Erkennungslücken aufgedeckt (ADR-013):

1. **`prev_*_success` als Parametername der Callable.** Airflow injiziert den Kontext über den
   Parameternamen. Gefunden in `V-Dang/covid_pipeline`, `archive.py:3`
   (`def get_last_execution_date(prev_start_date_success, **kwargs)`) und in
   `oxylabs/building-scraping-pipeline-apache-airflow`, `DAG/scrape.py:26`
   (`lambda prev_start_date_success: ...`). Das ist dieselbe Wartesemantik wie das Template.
2. **Template in einer Modul-Variablen.** Gefunden in `abdurahim-dag/portfolio`,
   `exchange rate/solution/dags/init.py:42`
   (`date_last_success = '{{ prev_start_date_success }}'`). Der Scanner sah Templates nur in
   Argumenten eines Aufrufs.

Beides ist jetzt DAG-scoped erfasst und getestet (`scanner/analyze_test.py`). Wirkung im vollen
Lauf: 5 zusätzliche Signal-F-Fundstellen in 5 Repos, davon 3 starke.

**`include_prior_dates` bleibt bei null DAGs, und das ist richtig.** Der Begriff kommt in den
geklonten Repos hunderte Male vor, aber praktisch immer als Parameter von `xcom_pull(...)` oder
im Airflow-Quelltext selbst. Von den Vorkommen außerhalb installierter Pakete steht kein
einziges an einem `ExternalTaskSensor`. Signal D ist in freier Wildbahn faktisch tot.
