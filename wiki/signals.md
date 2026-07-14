# Cross-Run-Signale A bis F

Diese Seite definiert exakt, was als Cross-Run-Kante zählt. Sie ist die gemeinsame Grundlage für den Scanner (Phase 1) und den Parser (Phase 2). Wenn Scanner und Parser unterschiedliche Definitionen benutzen, sind die Marktzahlen und die Produktzahlen nicht vergleichbar, und das Tool widerlegt seinen eigenen Launch-Content.

**Hinweis zur Herkunft:** Der ursprüngliche Auftrag verweist auf "Signale A bis F wie im Prototyp". Der Prototyp `crossrun_scan.py` existiert nicht (siehe ADR-001). Die folgende Liste ist daher unsere eigene Definition, hergeleitet aus der Airflow- und dbt-Semantik. Sie ist damit begründungspflichtig, nicht ererbt.

## Die Signale

### A — `depends_on_past=True`

Task in Lauf k startet erst, wenn dieselbe Task in Lauf k-1 erfolgreich war.

**Graph-Wirkung:** Selbst-Kante `task(k-1) → task(k)` mit Gewicht gleich der Dauer der Task.

**Fundstellen:** direkt am Operator, oder in `default_args`, dann gilt es für alle Tasks des DAG. Beide Ebenen müssen erkannt werden, und die Operator-Ebene überschreibt `default_args`.

**Fallstrick:** `depends_on_past=False` explizit gesetzt ist ein Treffer im Regex-Sinn und **kein** Signal. Nur der AST unterscheidet das zuverlässig.

### B — `wait_for_downstream=True`

Task in Lauf k startet erst, wenn die Task **und alle ihre direkten Downstream-Tasks** in Lauf k-1 erfolgreich waren. Impliziert `depends_on_past`.

**Graph-Wirkung:** Kante von jedem direkten Downstream-Nachfolger in Lauf k-1 auf die Task in Lauf k. Das ist strikt stärker als A und erzeugt in der Regel einen deutlich längeren Kreis, weil der Pfad durch die Downstream-Tasks mitzählt.

### C — `ExternalTaskSensor` mit Zeitversatz

Sensor wartet auf eine Task in einem **anderen** DAG.

**Nur dann Cross-Run, wenn** `execution_delta` oder `execution_date_fn` gesetzt ist. Ohne beides zeigt der Sensor auf denselben Logical Date, das ist eine Intra-Run-Kante zwischen zwei DAGs und **kein** Signal.

**Ebenfalls kein Signal: ein Versatz von null.** `execution_delta=timedelta(hours=0)` ist gesetzt, zeigt aber auf denselben Logical Date und ist damit dieselbe Intra-Run-Kante wie ein fehlender Versatz. Ein `timedelta`-Literal wird deshalb ausgerechnet, statt nur auf Anwesenheit geprüft zu werden (ADR-014, gefunden in der Stichprobe zu Session 003). Ein Versatz, der statisch nicht auflösbar ist, zählt weiterhin.

**Graph-Wirkung:** Kante vom Ziel-Task des fremden DAG bei Logical Date `t - execution_delta` auf den Sensor bei `t`. Wenn `execution_delta` ein Vielfaches der Schedule-Periode ist, spannt das einen Kreis über mehrere Perioden. Ein `execution_delta` von zwei Perioden erzeugt einen Kreis mit zwei Kanten, was das Zyklusmittel halbiert. Genau dieser Fall gehört als Test-Fixture in Phase 2.

**Grenze:** `execution_date_fn` ist eine beliebige Python-Funktion. Statisch ist ihr Rückgabewert im Allgemeinen nicht bestimmbar. Wir zählen sie als Cross-Run-Signal (der Zeitversatz ist ihr einziger Zweck), können aber das Gewicht nicht ableiten. Der Parser muss das als "Cross-Run erkannt, Versatz unbekannt" melden und darf nicht raten.

### D — `include_prior_dates=True`

Parameter am `ExternalTaskSensor`. Der Sensor akzeptiert auch Läufe mit früherem Logical Date.

**Graph-Wirkung:** Cross-Run, weil die Abhängigkeit explizit in die Vergangenheit greift. Wird unabhängig von C gezählt, weil ein Sensor `include_prior_dates=True` ohne `execution_delta` haben kann.

### E — dbt `is_incremental()`

Ein dbt-Model mit `materialized='incremental'`, dessen SQL `is_incremental()` aufruft, liest im inkrementellen Lauf aus seiner eigenen Zieltabelle, typischerweise über `select max(ts) from {{ this }}`.

**Graph-Wirkung:** Selbst-Kante `model(k-1) → model(k)`. Das Model kann nicht starten, bevor sein eigener Vorgänger-Lauf geschrieben hat.

**Fallstrick:** `materialized='incremental'` ohne `is_incremental()` im Body ist ein Full-Refresh-Model in inkrementeller Verkleidung und keine echte Rekurrenz. Umgekehrt ist `is_incremental()` in einem nicht-inkrementellen Model toter Code. Beides zählt nicht. Nur die Kombination zählt.

### F — Prior-Run-Templates

Jinja-Referenzen auf den vorigen Lauf in Templates, Operator-Argumenten oder SQL: `prev_ds`, `prev_execution_date`, `prev_start_date_success`, `prev_data_interval_start_success`, `prev_data_interval_end_success`.

**Drei Fundorte, dieselbe Semantik** (ADR-013, aus der Negativ-Suche in Session 003):

| Fundort | Beispiel | Herkunft |
|---|---|---|
| String-Literal im Operator-Argument | `bash_command="load --since {{ prev_start_date_success }}"` | der Regelfall |
| Parametername der Callable | `def load(prev_start_date_success, **kwargs)`, `lambda prev_start_date_success: ...` | Airflow injiziert den Kontext über den Parameternamen (`oxylabs/building-scraping-pipeline-apache-airflow`, `DAG/scrape.py:26`) |
| Template in einer Modul-Variablen | `date_last_success = '{{ prev_start_date_success }}'` | später ins Operator-Argument interpoliert (`abdurahim-dag/portfolio`, `.../dags/init.py:42`) |

Wer nur den ersten Fundort erkennt, misst die Verbreitung einer Schreibweise, nicht die des Signals.

**Graph-Wirkung:** Der Task liest Daten, die durch den vorigen Lauf definiert sind. Das ist eine echte Datenabhängigkeit über die Laufgrenze.

**Abstufung:** Die `*_success`-Varianten sind harte Kanten, weil sie auf den **erfolgreichen** Vorlauf verweisen und damit auf dessen Abschluss warten. `prev_ds` und `prev_execution_date` sind reine Datums-Arithmetik ohne Wartesemantik: sie zeigen eine Datenabhängigkeit an, erzwingen aber keine Reihenfolge. Sie werden deshalb als **schwaches Signal** geführt, getrennt gezählt und **nicht** in die Risiko-Kandidaten-Quote eingerechnet. Wer sie mitzählt, bläst die Marktzahl auf und liefert dem ersten kritischen Leser die Munition, um sie zu kippen.

## Was ausdrücklich kein Cross-Run-Signal ist

| Konstrukt | Warum nicht |
|---|---|
| `max_active_runs=1` | Serialisiert Läufe, erzeugt aber keine Datenabhängigkeit. Begrenzt die Nebenläufigkeit, nicht die Rekurrenz. Relevant für die reale Taktzeit, nicht für λ. |
| `ExternalTaskSensor` ohne Zeitversatz | Zeigt auf denselben Logical Date. Intra-Run. |
| `depends_on_past=False` | Explizite Verneinung. |
| `TriggerDagRunOperator` | Löst einen neuen Lauf aus, wartet aber nicht auf den vorigen. Kette, kein Kreis. |
| `catchup=True` | Backfill-Verhalten, keine Abhängigkeit. |
| Sensor auf externe Datenquelle | Wartet auf die Welt, nicht auf den eigenen Vorlauf. |

## Schedule-Klassifikation

Ein Signal allein ist harmlos. Gefährlich wird es erst, wenn der Schedule schneller taktet, als der Kreis erlaubt. **Risiko-Kandidat** ist deshalb definiert als: mindestens ein starkes Signal **und** sub-täglicher Schedule **im selben DAG**.

Stark sind A, B, C, D, E und F in den `*_success`-Varianten. Schwach sind `prev_ds`, `prev_ds_nodash` und `prev_execution_date`; sie werden getrennt gezählt und begründen für sich genommen keinen Risiko-Kandidaten (ADR-005, ADR-011).

Sub-täglich heißt: Periode kürzer als 24 Stunden.

| Schedule-Form | Beispiel | Sub-täglich |
|---|---|---|
| Preset | `@hourly` | ja |
| Preset | `@daily`, `@weekly`, `@monthly` | nein |
| Preset | `@once`, `None` | nein, kein Schedule |
| Preset | `@continuous` | ja, die Periode liegt per Definition unter einem Tag |
| Cron, Minuten-Feld mit Schritt | `*/15 * * * *` | ja |
| Cron, Stunden-Feld mit Schritt | `0 */6 * * *` | ja |
| Cron, Stunden-Feld als Liste | `0 6,18 * * *` | ja |
| Cron, feste Stunde | `0 3 * * *` | nein |
| `timedelta` | `timedelta(hours=4)` | ja |
| `timedelta` | `timedelta(days=1)` | nein |
| Dataset- oder Asset-getriggert | `schedule=[Dataset(...)]` | unbekannt, eigene Kategorie |

Cron-Ausdrücke werden nicht per Heuristik am String klassifiziert, sondern über die berechnete kleinste Distanz zwischen zwei aufeinanderfolgenden Feuerzeitpunkten. Das ist die einzige Methode, die bei Listen, Schritten und Kombinationen zuverlässig bleibt, und sie ist mit einer Tabelle aus Beispielen testbar. Implementiert in `scanner/schedule.py`, gerechnet über ein Fenster von fünf Jahren, ohne Cron-Bibliothek (ADR-010). Ein Ausdruck, der in diesem Fenster nie feuert, ist `unknown` und wird nicht geraten.
