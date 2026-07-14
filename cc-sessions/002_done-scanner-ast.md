# Session 002 — Scanner: AST-Analyse

**Phase 1, Schritt 2 von 3.** Ziel ist die Analyse-Funktion, nicht der große Lauf. Der kommt in 003.

## Vorher lesen

- `wiki/signals.md` — die Signal-Definitionen. Sie sind verbindlich. Wenn beim Implementieren auffällt, dass eine Definition Unsinn ist: Wiki korrigieren und ADR schreiben, nicht heimlich anders implementieren.
- `CLAUDE.md`, harte Regeln 4, 5, 6

## Auftrag

Baue `scanner/clone.py` und `scanner/analyze.py`.

### 1. Clone-Schicht

`git clone --depth 1` in einen Disk-Cache unter `data/repos/<org>__<repo>/`. Vorhandene Clones werden nicht neu gezogen. Fehlgeschlagene Clones (privat geworden, gelöscht, Timeout) landen in `scan_errors.jsonl` und der Lauf geht weiter.

Timeout pro Clone: 120 s. Danach abbrechen, loggen, weiter. Ein einzelnes hängendes Repo darf einen Lauf über 350 Repos nicht blockieren.

### 2. DAG-Erkennung

Ein Python-File ist ein DAG-File, wenn im AST eines davon vorkommt:
- Instanziierung `DAG(...)` oder `models.DAG(...)`
- `with DAG(...) as dag:`
- Ein Funktions-Decorator `@dag` oder `@airflow.decorators.dag`

Kein Regex. `ast.parse` mit `SyntaxError`-Behandlung: fremde Repos enthalten Python 2, kaputte Files und Templates mit Platzhalter-Syntax. Ein `SyntaxError` ist erwartbar, wird geloggt und ist kein Absturz (siehe CLAUDE.md, Systemgrenzen).

### 3. DAG-Scoping (das ist der Kern)

Ein File kann mehrere DAGs enthalten. Jedes Signal wird dem DAG zugeordnet, in dessen Kontext es steht:

- **`with DAG(...) as x:`** — alles im Body gehört zu diesem DAG.
- **`dag = DAG(...)`** plus Operatoren mit `dag=dag` — Zuordnung über die Variable.
- **`@dag`-Decorator** — alles im Funktionskörper gehört zu diesem DAG.
- **Operator ohne erkennbare DAG-Zuordnung in einem File mit genau einem DAG** — dem DAG zuordnen, aber mit Konfidenz-Flag `inferred`.
- **Operator ohne Zuordnung in einem File mit mehreren DAGs** — **nicht** zuordnen. In `scan_errors.jsonl` als `ambiguous_task` loggen. Raten ist hier verboten, das ist genau die Sorte False Positive, die den Report kippt.

`default_args` wird pro DAG aufgelöst. `depends_on_past` in `default_args` gilt für alle Tasks des DAG. Ein Operator-Argument überschreibt `default_args`. Wenn `default_args` aus einer Variable kommt, die im selben File als Dict-Literal definiert ist: auflösen. Wenn sie importiert wird oder dynamisch entsteht: nicht auflösen, als `unresolved_default_args` loggen. Nicht raten.

### 4. Signale

Implementiere A bis F exakt nach `wiki/signals.md`. Die drei Fallen, an denen ein Regex-Scanner scheitert und an denen dieser hier gemessen wird:

- `depends_on_past=False` ist **kein** Signal.
- `ExternalTaskSensor` **ohne** `execution_delta` und **ohne** `execution_date_fn` ist **kein** Cross-Run-Signal.
- Ein Vorkommen in Kommentar, Docstring oder String-Literal ist **kein** Signal. Der AST löst das automatisch, solange nirgends auf den Rohtext zurückgefallen wird.

Für jedes gefundene Signal: **Datei plus Zeilennummer** aus `node.lineno`. Ohne Zeilennummer kein Treffer (CLAUDE.md, Regel 6).

**Pfade werden vollständig protokolliert, nie gekürzt.** Bei der Abnahme von 001 stand im Log `dags/tutorial.py:35`, der echte Pfad war `docker/sandbox/ubuntu-airflow/airflow/dags/tutorial.py`. Der Beleg ließ sich damit nicht in dreißig Sekunden auflösen, und genau das verlangt Regel 6. In `scan_results.csv` und in jedem Report steht der volle Repo-Pfad.

### 4b. Task-Factories (nachgetragen bei der Abnahme von 001, ADR-009)

Die Stichprobe aus 001 hat ein Muster aufgedeckt, das die ursprüngliche Spec übersehen hätte. In `navikt/team_familie_airflow_dags`, `operators/kafka_operators.py:32-33` steht:

```python
def kafka_consumer_kubernetes_pod_operator(
    ...,
    depends_on_past: bool = True,
    wait_for_downstream: bool = True,
    ...,
):
    return KubernetesPodOperator(...)
```

Jeder Task aus dieser Factory trägt beide starken Signale. Das Signal ist echt, es steht nur in einem Helper-Modul, das **kein DAG instanziiert**. Nach der Regel "scanne DAG-Files, ordne DAG-scoped zu" würde der Scanner dieses Repo als signalfrei melden. Das wäre ein Falsch-Negativ mit Ansage.

**Also:** Python-Files, die kein DAG-File sind, werden trotzdem auf dieses eine Muster geprüft. Erkennungsregel, bewusst schlicht: eine Funktion, die einen Operator oder Sensor instanziiert und zurückgibt, und die ein Signal-Schlüsselwort als Parameter-Default `True` führt oder es an den Operator durchreicht.

Treffer werden als `factory_signal` mit Datei und Zeile protokolliert und **getrennt gezählt**. Sie fließen **nicht** in die DAG-scoped Risiko-Quote, weil sie sich keinem DAG zuordnen lassen, ohne die Aufrufstellen zurückzuverfolgen. Genau diese Rückverfolgung wird **nicht** gebaut (Begründung in ADR-009: `**kwargs`, dynamische Imports, Schleifen, unverhältnismäßig für eine Marktzahl).

Die Konsequenz ist, dass unsere Hauptquote eine **Untergrenze** ist. Das ist die richtige Fehlerrichtung, aber es muss im Report stehen, und die Factory-Zahl gehört daneben.

### 5. Schedule-Klassifikation

`scanner/schedule.py`, eigene getestete Funktion. Nimmt `schedule`, `schedule_interval` oder `timetable` aus dem DAG-Aufruf und liefert:

```python
Literal["subdaily", "daily_or_slower", "none", "dataset_triggered", "unknown"]
```

Cron wird **nicht** am String geraten. Berechne die kleinste Distanz zwischen aufeinanderfolgenden Feuerzeitpunkten. Wenn dafür eine Cron-Bibliothek nötig ist (`croniter`), ist das die eine erlaubte Zusatz-Dependency für den Scanner, aber **nicht** für das `eigenlag`-Package. Begründung im Session-Log.

Testtabelle als Minimum (aus `wiki/signals.md`):

| Eingabe | Erwartet |
|---|---|
| `"@hourly"` | subdaily |
| `"@daily"` | daily_or_slower |
| `"*/15 * * * *"` | subdaily |
| `"0 */6 * * *"` | subdaily |
| `"0 6,18 * * *"` | subdaily |
| `"0 3 * * *"` | daily_or_slower |
| `timedelta(hours=4)` | subdaily |
| `timedelta(days=1)` | daily_or_slower |
| `None` | none |
| `[Dataset("s3://...")]` | dataset_triggered |

### 6. dbt-Analyse

`scanner/analyze_dbt.py`. Ein Model zählt nur dann als Signal E, wenn **beides** zutrifft: `materialized='incremental'` (aus `dbt_project.yml`, aus dem `{{ config(...) }}`-Block im SQL, oder aus einer `.yml` im Model-Ordner) **und** `is_incremental()` im SQL-Body. Nur eines von beidem zählt nicht (siehe `wiki/signals.md`).

SQL wird nicht per AST geparst, das ist unverhältnismäßig. Hier ist Textsuche vertretbar, **aber** Kommentare (`--` und `/* */`) werden vorher entfernt. Das im Session-Log begründen und die Kommentar-Entfernung testen.

## Akzeptanz

- Fixture-Repo unter `scanner/fixtures/` mit von Hand gebauten DAG-Files, die jede Falle enthalten: zwei DAGs in einem File, `depends_on_past=False`, `depends_on_past` in einem Kommentar, `ExternalTaskSensor` mit und ohne `execution_delta`, `default_args`-Vererbung, ein File mit `SyntaxError`.
- `pytest` grün gegen diese Fixtures, mit gepastetem Output im Session-Log.
- Jede Signal-Meldung trägt Datei und Zeilennummer.
- `ruff` und `mypy` grün.

## Explizit nicht in dieser Session

Der große Lauf, das CSV, `report.md`. Das ist 003.
