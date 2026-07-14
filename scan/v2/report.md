# Scan-Report — Cross-Run-Abhängigkeiten in öffentlichen Airflow- und dbt-Repos

Lauf über 1692 Kandidaten-Repos aus `data/candidates.jsonl`. Alle Zahlen mit Nenner, jeder Treffer mit Permalink auf den Commit-SHA des Clones.

## Airflow

### Lauf

| Kennzahl | Wert |
|---|---|
| Repos in der Kandidatenliste | 1692 |
| davon geklont | 1691 (99.9 %) |
| Clone fehlgeschlagen | 1 |
| Python-Files geparst | 318495 |
| davon `SyntaxError` (protokolliert, kein Abbruch) | 7593 |
| Repos mit mindestens einem DAG | 1287 |

Fehler-Kategorien aus dem Fehler-Log des Laufs (Regel 7):

| Kategorie | Vorkommen |
|---|---|
| `syntax_error` | 7593 |
| `unresolved_default_args` | 5629 |
| `ambiguous_task` | 434 |
| `yaml_error` | 29 |
| `clone_failed` | 1 |

### Befund

**Nenner ist der DAG, nicht das Repo und nicht das File: 51789 DAGs.**

| Kennzahl | Absolut | Anteil an allen DAGs |
|---|---|---|
| DAGs mit Cross-Run-Kante (starkes Signal) | 1303 | 2.5 % |
| DAGs mit sub-täglichem Schedule | 2543 | 4.9 % |
| **Risiko-Kandidaten Kern (Signal aus A–F **und** sub-täglich im selben DAG)** | **176** | **0.3 %** |
| Risiko-Kandidaten nur Signal G (`max_active_runs=1` **und** sub-täglich, kein A–F-Signal; ADR-018) | 473 | 0.9 % |
| Repos mit mindestens einem Kern-Kandidaten | 100 | — |
| Repos mit mindestens einem G-only-Kandidaten | 159 | — |
| DAGs ohne `dag_id` (Konstruktor-Aufruf, die id setzt erst der Aufrufer; ADR-015) | 4952 | 9.6 % |

**Die zwei Klassen sind bewusst getrennt (ADR-018).** Die Kern-Quote ist definitionsgleich mit Session 003 und bleibt die Launch-Zahl: dort ist der Kreis ein Teilpfad, λ < Makespan ist möglich, und kein heutiges Tool beantwortet das. Bei den G-only-Kandidaten ist die Kante real, aber λ = Makespan. Dort reicht Laufzeit-Monitoring, und der Report sagt das selbst, bevor es ein Kritiker tut.

### Vorher/Nachher gegen Session 003

Zwischen den beiden Läufen liegen zwei Änderungen: ADR-015 (repo-eigene DAG-Konstruktoren werden erkannt) und ADR-016/018 (Signal G als neue, getrennt ausgewiesene Klasse). Die Definition der Kern-Quote ist unverändert. Jedes Delta ist einer Ursache zugeordnet; die Stichproben dazu stehen in `sample_verification.md`.

| Größe | 003 (alt) | 006 (neu) | Ursache |
|---|---|---|---|
| DAGs gefunden | 51426 | 51789 | ADR-015: +422 Konstruktor-Aufrufstellen, −59 Schablonen in Konstruktor-Rümpfen |
| DAGs mit Cross-Run-Kante (A–F) | 1303 | 1303 | unverändert, Definition und Treffer-Menge identisch |
| Risiko-Kandidaten (Kern) | 176 | 176 | unverändert, Definition und Treffer-Menge identisch |
| Risiko-Kandidaten (nur G) | — | 473 | neue Klasse (ADR-016, ADR-018), in 003 nicht erhoben |
| Repos mit Kern-Kandidat | 100 | 100 | folgt den Kern-Kandidaten |

Eine Einschränkung zum DAG-Delta, damit sie niemand zuerst findet: **330 der 363 netto neuen "DAGs" sind keine.** Sie stammen aus einem einzigen Repo (`mik-laj/airflow-api-clients`), einem generierten OpenAPI-Client, dessen Modellklasse zufällig `DAG` heißt — ADR-015 befördert jede Funktion, die ein `DAG(...)` zurückgibt, und kann eine repo-lokale Klasse dieses Namens nicht von Airflows `DAG` unterscheiden. Alle 330 Zeilen sind signalfrei, der Zähler ist also unberührt; sie verwässern nur den Nenner um 0,6 %. Ein Import-Check (`DAG` aus `airflow` importiert oder lokal definiert?) würde das beheben und ist als ADR-Kandidat für die nächste Scanner-Session vorgemerkt (siehe `STATUS.md`), nach der Zaun-Regel aus Spec 006: keine neue Erkennungsregel in derselben Session, deren Lauf sie veralten würde.

Schedule-Klassen:

| Klasse | DAGs | Anteil |
|---|---|---|
| `daily_or_slower` | 27617 | 53.3 % |
| `none` | 18757 | 36.2 % |
| `unknown` | 2676 | 5.2 % |
| `subdaily` | 2543 | 4.9 % |
| `dataset_triggered` | 196 | 0.4 % |

Signale, je DAG gezählt (ein DAG kann mehrere tragen):

| Signal | DAGs | Anteil | In der Quote |
|---|---|---|---|
| `sig_a_depends_on_past` | 958 | 1.8 % | ja, Kern |
| `sig_b_wait_for_downstream` | 192 | 0.4 % | ja, Kern |
| `sig_c_ext_sensor_delta` | 318 | 0.6 % | ja, Kern |
| `sig_d_include_prior_dates` | 0 | 0.0 % | ja, Kern |
| `sig_f_prev_success_tmpl` | 5 | 0.0 % | ja, Kern |
| `sig_f_weak_prev_ds` | 35 | 0.1 % | nein (ADR-005) |
| `sig_g_max_active_runs` | 3529 | 6.8 % | eigene Klasse (ADR-018) |

### Beispiele (Risiko-Kandidaten, ein DAG je Repo, nach Sternen sortiert)

| Repo | DAG | Schedule | Signale | Beleg |
|---|---|---|---|---|
| `jcw024/lichess_database_ETL` (212★) | `lichess_ETL_pipeline_kafka` | `timedelta(minutes=5)` | depends_on_past, max_active_runs | [src/airflow_dag_kafka.py:48](https://github.com/jcw024/lichess_database_ETL/blob/9596bfdb0167f1e86ccc71d168aa354fee617654/src/airflow_dag_kafka.py#L48) |
| `aliyun/dataworks-spec` (27★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [client/migrationx/migrationx-reader/src/main/python/src/test/test_dag_folder/example_branch_python_dop_operator_3.py:34](https://github.com/aliyun/dataworks-spec/blob/a92e8ae5d5bcd4d7c598ae8510f8ab2c897aad4b/client/migrationx/migrationx-reader/src/main/python/src/test/test_dag_folder/example_branch_python_dop_operator_3.py#L34) |
| `BigDataMatrix/DataPipeline` (6★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/example_dags/example_branch_python_dop_operator_3.py:34](https://github.com/BigDataMatrix/DataPipeline/blob/068de6c1868631aa07ae0df87cfb106ee19e7bb9/airflow/example_dags/example_branch_python_dop_operator_3.py#L34) |
| `SonarSource/python-test-sources` (6★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/airflow/example_dags/example_branch_python_dop_operator_3.py:49](https://github.com/SonarSource/python-test-sources/blob/117bb5d06d8bae21cf091d7114071be9e53d4315/airflow/airflow/example_dags/example_branch_python_dop_operator_3.py#L49) |
| `Ricochet-Exchange/ricochet-keeper` (5★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/example_dags/example_branch_python_dop_operator_3.py:49](https://github.com/Ricochet-Exchange/ricochet-keeper/blob/200b15fb2c017b415c87bf732874663e8a16c438/airflow/example_dags/example_branch_python_dop_operator_3.py#L49) |
| `DmitriiDenisov/airflow-lab` (3★) | `branch_list_ex` | `timedelta(minutes=2)` | depends_on_past | [dags_examples/branch_list_ex.py:39](https://github.com/DmitriiDenisov/airflow-lab/blob/a9082852a13db77d96b5e8dde2b9bd40e8fc1d0f/dags_examples/branch_list_ex.py#L39) |
| `a0x8o/airflow` (3★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/example_dags/example_branch_python_dop_operator_3.py:49](https://github.com/a0x8o/airflow/blob/044e5a148e8cb0fd091684189411d277f3ffa03b/airflow/example_dags/example_branch_python_dop_operator_3.py#L49) |
| `beefjerky/new_airflow` (3★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/example_dags/example_branch_python_dop_operator_3.py:31](https://github.com/beefjerky/new_airflow/blob/6cb785c8a97829833db5293ad3458dd16fcbf68c/airflow/example_dags/example_branch_python_dop_operator_3.py#L31) |
| `kira-lin/ve450-declarative-deployment-framework` (2★) | `test_dag_v1` | `'*/10 * * * *'` | depends_on_past | [airflow/dags/test_dag.py:35](https://github.com/kira-lin/ve450-declarative-deployment-framework/blob/f28e8b468568c8623134db5a1a8757860788799f/airflow/dags/test_dag.py#L35) |
| `wanman/incubator-airflow` (2★) | `example_branch_dop_operator_v3` | `'*/1 * * * *'` | depends_on_past | [airflow/example_dags/example_branch_python_dop_operator_3.py:31](https://github.com/wanman/incubator-airflow/blob/7764c75a7c8d1cd3d4a3f9ce021d92988bb45698/airflow/example_dags/example_branch_python_dop_operator_3.py#L31) |

## dbt

Getrennt ausgewertet, mit eigenem Nenner. Ein dbt-Model enthält keinen Schedule: wie oft es läuft, steht in Airflow, in dbt Cloud oder in einem Cron außerhalb des Repos. Die Risiko-Bedingung (starkes Signal **und** sub-täglich im selben DAG) ist hier konstruktionsbedingt nicht auswertbar, deshalb taucht kein dbt-Model in der Airflow-Quote auf (ADR-012).

**Die dbt-Zahlen sind aus Session 003 übernommen.** ADR-015 und ADR-016 sind Airflow-seitig, `analyze_dbt.py` ist unverändert; der Re-Lauf über dieselben Clones reproduziert dieselben Werte.

| Kennzahl | Wert |
|---|---|
| Repos mit `dbt_project.yml` | 498 |
| Models gefunden | 37124 |
| Models mit echter Selbst-Kante (`materialized='incremental'` **und** `is_incremental()`) | 3369 (9.1 %) |
| Repos mit mindestens einem solchen Model | 363 |

Woher die Materialisierung kam:

| Quelle | Models |
|---|---|
| `config_block` | 3168 |
| `schema_yml` | 104 |
| `dbt_project` | 97 |

**Bei dbt kennen wir den Kreis, aber nicht den Takt.** Genau deshalb ist ein Werkzeug nötig, das beides zusammenbringt.

## Untergrenzen: wo die Quote zu klein ist

Alle drei Posten zeigen in dieselbe Richtung: der Scanner findet weniger, als da ist. Keiner davon wird in den Zähler gerechnet.

| Posten | Zahl | Warum nicht in der Quote |
|---|---|---|
| Task-Factories (ADR-009) | 4 Signale in 2 Repos | Ohne interprozedurale Analyse keinem DAG zuzuordnen. Die Signale sind echt, siehe `scan_factories.csv`. |
| `unresolved_default_args` | 5629 Fälle in 148 Repos | `default_args` aus Import oder dynamischer Konstruktion. Ein `depends_on_past=True` darin ist unsichtbar, wird aber nicht geraten. |
| Ambige Tasks | 434 Fälle in 50 Repos | Operator ohne DAG-Bindung in einem File mit mehreren DAGs. Wird nicht geraten. |

## Was diese Zahlen nicht sagen

- **Der 1000er-Deckel.** 4 der 6 Code-Search-Queries laufen in das Limit der GitHub-Code-Search von 1000 ausgelieferten Ergebnissen: `depends_on_past` meldet 2284 Treffer, `wait_for_downstream` meldet 1168 Treffer, `include_prior_dates` meldet 1076 Treffer, `is_incremental` meldet 1896 Treffer. Geholt wurden je 1000. Die Stichprobe ist nach oben abgeschnitten.
- **Die Stichprobe ist keine Zufallsauswahl** aus allen Airflow-Nutzern, sondern aus öffentlichen Repos, die bestimmte Begriffe enthalten und über die Code-Search auffindbar sind.
- **Öffentliche Repos sind keine Produktions-Pipelines, und das ist hier keine Floskel.** 38942 der 51789 DAGs (75.2 %) liegen in einem Pfad mit `example`, `test`, `tutorial`, `docs` oder `sample`, oder tragen eine `dag_id`, die mit `example_` beginnt. Unter den 176 Risiko-Kandidaten sind es 138 (78.4 %). Die Stichprobe zeigt, woran das liegt: der Airflow-eigene Beispiel-DAG `example_branch_dop_operator_v3` trägt `depends_on_past=True` bei `*/1 * * * *` und wird in jedes zweite Lern-Repo kopiert. Die Marker sind eine grobe Heuristik, sie korrigieren keine Zahl. Sie beziffern, wie stark Zähler und Nenner von Anschauungsmaterial getragen werden, und dieser Anteil ist der wichtigste Vorbehalt gegen jede Aussage über den Markt.
- **`fork` und `archived` haben null Mal gegriffen** (an 20 Kandidaten nachgeprüft, Session 001). Die klassische Code-Search liefert diese Repos offenbar nicht aus. Kein Filter-Fehler, aber ohne Erklärung liest es sich wie einer.
- **Die Blocklist verwirft 251 Repos**, der Größenfilter weitere 152, zusammen 403. Jede Verwerfung steht mit Grund in `data/rejected.jsonl` und ist damit anfechtbar.
- **Der Scanner sagt nicht, dass diese Pipelines instabil sind.** Er sagt, dass sie die Struktur haben, in der Instabilität entstehen kann, und dass kein Werkzeug ihnen zeigt, ob sie es sind.
- **Die Definition hat sich zwischen den Läufen geändert, und das steht hier absichtlich.** Signal G (`max_active_runs=1`) kam nach dem ersten Scan dazu, mit gemessener Begründung: der Wikimedia-Fall hat gezeigt, dass die Kante real bindet (ADR-016). Es wurde als eigene Klasse ausgewiesen statt in die Kern-Quote gemischt (ADR-018); die Kern-Quote ist definitionsgleich mit Session 003 geblieben.
- **G-only heißt: Laufzeit-Monitoring reicht dort.** Für einen DAG, dessen einzige Cross-Run-Kante `max_active_runs=1` ist, gilt λ = Makespan: die Taktgrenze ist die Laufzeit selbst, und die zeigt jedes Dashboard. Der Analyzer verdient sein Geld erst, wo der Kreis ein Teilpfad ist und λ < Makespan sein kann. Genau deshalb stehen die beiden Klassen getrennt.

## Die Aussage, die nicht an der Prozentzahl hängt

> Wir haben 1303 Airflow-DAGs mit einem Kreis über die Zeitachse gefunden, dazu 3369 dbt-Models mit einer Selbst-Kante. Für keinen einzigen davon ist bekannt, wo seine Taktgrenze liegt, weil kein Werkzeug sie ausrechnet.

