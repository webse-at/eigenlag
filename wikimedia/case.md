# Der erste echte Fall: Wikimedias stündlicher Reconcile-DAG

Wikimedia betreibt Airflow in Produktion, und beides liegt offen: der DAG-Code auf GitLab, die
gemessenen Laufzeiten in einem anonym abfragbaren Prometheus. Damit lässt sich λ zum ersten Mal
an einer echten Pipeline ausrechnen und gegen ihren tatsächlichen Takt halten.

**Das Ergebnis in einem Satz:** `wdqs_streaming_updater_reconcile_hourly` läuft im Stundentakt,
und seine mittlere Laufzeit liegt bei 3598,4 Sekunden. Der Takt beträgt 3600 Sekunden. Die
Pipeline arbeitet mit einem Abstand von **1,6 Sekunden pro Lauf** an ihrer eigenen Taktgrenze,
und der Preis dafür ist eine Verspätung von 48 Minuten, die nie mehr verschwindet.

Alle Zahlen unten stammen aus `data/wikimedia/case_numbers.json`, erzeugt von
`python -m wikimedia.case`. Jede Rohantwort liegt in `data/wikimedia/cache/`.

---

## 1. Der Code

Repo: `https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags.git`, Stand
`6d0cceb85e4a21d593638f6b9e5694e5f4dbc013` (14. Juli 2026). Die Permalinks unten zeigen auf
genau diesen Commit.

Der DAG entsteht in [`search/dags/rdf_streaming_updater_reconcile.py`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L112-121):

```python
112    with create_easy_dag(
113            dag_id=dag_id,
114            start_date=datetime(2024, 2, 20, 7, 00, 00),
115            default_args={
116                'depends_on_past': True,
117            },
118            schedule='@hourly',
119            # We want hourly runs to be scheduled one ofter the other
120            max_active_runs=1,
121            catchup=True,
```

Die Funktion `build_dag` ([Zeile 106](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L106))
wird zweimal aufgerufen und erzeugt zwei DAGs:
[`wdqs_…`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L140)
und [`wcqs_…`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L147).
Der Taskgraph eines Laufs ([Zeile 135](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L135)):

```python
wait_for_data >> job >> complete
```

Drei Hive-Partition-Sensoren warten auf die Daten der laufenden Stunde, ein Spark-Job schreibt
die Reconciliation-Events, ein leerer Task schließt ab.

**Zwei Kanten führen diesen DAG im Kreis über die Zeitachse:**

- `depends_on_past: True` (Zeile 116) hängt jeden Task an denselben Task des Vorlaufs.
- `max_active_runs=1` (Zeile 120) hängt den ganzen Lauf an den vorigen: Lauf k kann nicht
  beginnen, bevor Lauf k−1 fertig ist. Der Kommentar darüber sagt es selbst: *"We want hourly
  runs to be scheduled one ofter the other."*

Der Takt ist `@hourly`. `eigenlag` rechnet daraus **T = 3600 s** (kleinste Distanz zweier
Feuerzeitpunkte von `0 * * * *`, siehe `scanner/schedule.py:period_seconds`), statt die Zahl
einzutragen.

## 2. Die Messung

Datenquelle: `https://grafana.wikimedia.org/api/datasources/proxy/uid/000000026/api/v1`,
anonym abfragbar. Fenster: 30 Tage, Ende 2026-07-15 00:00 UTC.

### Die Gauge-Frage, und warum `avg_over_time` hier nicht taugt

`airflow_dagrun_duration` ist eine Gauge: Airflow meldet am Ende eines Laufs dessen Dauer an
StatsD, der Exporter hält den Wert, Prometheus scrapt ihn. Ein `avg_over_time` darüber mittelt
**Scrapes, nicht Läufe**. Ob das dasselbe ist, hängt davon ab, wie lange jeder Wert stehen
bleibt, und das ist eine Annahme, keine Zusicherung.

Deshalb rekonstruiert `wikimedia/runs.py` die Läufe selbst: **ein Wertwechsel der Gauge ist ein
Lauf.** Die Dauern sind Fließkomma-Millisekunden mit Nachkommastellen, zwei Läufe mit exakt
gleichem Wert sind praktisch ausgeschlossen. Der Wechsel wird je Serie gesucht, nicht auf der
zusammengeführten Zeitachse, weil Wikimedia mehrere StatsD-Pods betreibt und jeder seinen
eigenen letzten Wert hält.

**Zwei unabhängige Prüfungen, dass die Lesart trägt:**

1. Serverseitig gerechnet ergibt `sum by (dag_id) (changes(airflow_dagrun_duration{state="success"}[30d]))`
   für wdqs **397**, unsere Rekonstruktion aus den Rohsamples **398**. Zwei Verfahren, dieselbe
   Zahl.
2. Die mediane Laufzeit (3733,8 s) und der mediane Abstand zweier Laufenden (3720 s) liegen
   13,8 Sekunden auseinander, also unter der Scrape-Auflösung von einer Minute. Das müssen sie,
   wenn `max_active_runs=1` die Läufe wirklich hintereinander legt, und der Abstand stammt aus
   den Zeitstempeln, nicht aus den Dauern: eine Größe, die die andere prüft.

Die Einheit ist damit ebenfalls belegt: die Werte sind Millisekunden, nicht Sekunden, sonst
könnten Dauer und Abstand nicht zusammenfallen.

### `wdqs_streaming_updater_reconcile_hourly`

PromQL: `airflow_dagrun_duration{dag_id="wdqs_streaming_updater_reconcile_hourly",state="success"}[30d]`

Lückenfreies Fenster: 2026-06-15 00:50 bis 2026-07-01 13:47 UTC, 16,5 Tage, **398 Läufe**.

| Größe | Wert |
|---|---|
| Takt T (aus `@hourly`) | 3600,0 s |
| Laufzeit, Median | 3733,8 s (62,2 min) |
| Laufzeit, Mittel | **3598,4 s (60,0 min)** |
| Laufzeit, p95 | 3778,8 s (63,0 min) |
| Laufzeit, min / max | 104,6 s / 7432,5 s |
| Beobachteter Takt (Abstand der Laufenden) | 3599,5 s |

### `wcqs_streaming_updater_reconcile_hourly`

712 Läufe, verteilt auf drei Fenster (zwei Metrik-Ausfälle dazwischen), letzter Lauf
2026-07-14 14:53 UTC. Median 3722,5 s, Mittel 3182,9 s, p95 3777,8 s. Der Mittelwert liegt
niedriger, weil dieser DAG nach den Ausfällen einen Rückstand aufgeholt hat; sein längster Lauf
dauerte 400.132 s, also 4,6 Tage.

**wdqs hat seit dem 1. Juli 2026 keinen erfolgreichen Lauf mehr gemeldet.** Der letzte Erfolg
liegt am 01.07. um 13:47 UTC, kurz darauf steht ein Fehlschlag über 108,9 Minuten. Seit dem
6. Juli hält seine `airflow_dagrun_schedule_delay`-Gauge einen einzigen eingefrorenen Wert. Ob
der DAG steht, pausiert wurde oder nur seine Metrik fehlt, ist von außen nicht zu entscheiden,
und wir behaupten es nicht. Die 398 Läufe davor sind davon unberührt.

## 3. λ

Modell (`wikimedia/case.py:lambda_of`): auf DAG-Ebene fallen beide Kreis-Kanten zusammen. Lauf k
kann nicht beginnen, bevor Lauf k−1 fertig ist, und das Gewicht des Kreises ist die Laufzeit.
Der Graph hat einen Knoten und eine Kante auf sich selbst, gerechnet wird er mit `eigenlag`
(Howard, `eigenlag/maxplus.py`), nicht von Hand.

Warum nur DAG-Ebene: die Task-Dauern gibt die Metrik nicht her. Für den Spark-Task und den
Abschluss-Task existiert **keine** Dauer-Metrik, `airflow_task_duration` trägt weder `dag_id`
noch `task_id`, und die drei Sensoren melden Dauern nahe null (Median 0,0 min, Maximum 0,1 min),
weil sie im Reschedule-Modus laufen: ihr Warten steckt nicht in ihrer Task-Dauer. Eine
Aufteilung der 62 Minuten auf die Tasks wäre geraten, und geraten wird nicht.

| λ aus | λ | Drift je Lauf (λ − T) |
|---|---|---|
| Median-Laufzeit | 3733,8 s | **+133,8 s** |
| mittlere Laufzeit | 3598,4 s | **−1,6 s** |
| p95-Laufzeit | 3778,8 s | +178,8 s |

Für die Frage, ob die Verspätung unbegrenzt wächst, zählt der **Mittelwert**: bei zufälligen
Laufzeiten ist das mittlere Kreisgewicht die Rate, mit der die Verspätung pro Lauf zunimmt
(`wiki/math.md`, Abschnitt 7). Er liegt 1,6 Sekunden **unter** dem Takt. Der DAG ist stabil,
aber die Reserve beträgt 0,04 Prozent.

Der beobachtete Takt bestätigt es unabhängig: die Läufe enden im Schnitt alle **3599,5 s**
auseinander. Der DAG liefert exakt einen Lauf pro Stunde, mehr geht nicht, und mehr ist auch
nicht nötig. Er sitzt auf seiner Taktgrenze.

**Der Preis steht in derselben Metrik.** `airflow_dagrun_schedule_delay` misst, wie lange nach
seinem logischen Zeitpunkt ein Lauf tatsächlich startet. Median: **2880 s, also 48 Minuten.**
Diese Verspätung wächst nicht mehr, sie verschwindet aber auch nicht. Sie ist genau das, was
eine Pipeline an ihrer Taktgrenze zeigt: sie hält den Takt, aber dauerhaft eine Dreiviertelstunde
zu spät.

## 4. Warum die Pipeline trotz Median über dem Takt nicht wegdriftet

Die mediane Laufzeit liegt 134 Sekunden über dem Takt. Wären die Laufzeiten unabhängig vom
Startzeitpunkt, müsste die Verspätung wachsen. Sie tut es nicht, und die Metrik zeigt, warum.

**Korrelation zwischen Verspätung beim Start und Laufzeit: −0,504** (397 Paare, wdqs). Je später
ein Lauf beginnt, desto kürzer läuft er. Der Grund steht im Code: die Sensoren warten auf die
Hive-Partitionen **der laufenden Stunde**. Startet ein Lauf pünktlich, wartet er auf Daten, die
es noch nicht gibt. Startet er 50 Minuten zu spät, liegen die Daten längst da, und er ist in
Minuten durch (kürzester Lauf: 104,6 s).

Dieser Sensor ist damit **keine Bearbeitungszeit, sondern eine Synchronisation mit der Uhr.** Er
wirkt als negative Rückkopplung und bricht den Kreis genau dann, wenn die Verspätung groß genug
geworden ist. Das ist der Grund, warum die reine Max-Plus-Annahme (Dauern unabhängig vom
Startzeitpunkt) hier an ihre Grenze kommt, und es ist eine Grenze, die man kennen muss, bevor
man einer Pipeline Drift bescheinigt. Wir haben sie in `wiki/math.md`, Abschnitt 9,
aufgeschrieben.

Bei wcqs ist dieselbe Korrelation nur −0,103, weil dort zwei Metrik-Ausfälle und ein 4,6 Tage
langer Lauf die Reihe dominieren.

## 5. Was der Scanner vorher nicht sah

Vor dieser Session fand `eigenlag` in Wikimedias Repo **71 von 325 produktiven DAGs und null
Cross-Run-Signale**, obwohl `depends_on_past=True` dort mehrfach im Klartext steht. Grund:
Wikimedia erzeugt DAGs nicht über `DAG(...)`, sondern über `create_easy_dag(...)`, eine Methode,
die intern ein `DAG(...)` zurückgibt
([`wmf_airflow_common/easy_dag.py:79`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/wmf_airflow_common/easy_dag.py#L79)).

| | vorher | nach ADR-015 | nach ADR-016 |
|---|---|---|---|
| DAGs gefunden | 71 | 345 | 345 |
| davon mit `dag_id` | 58 | 255 | 255 |
| mit Cross-Run-Signal | 0 | 13 | 68 |
| Risiko-Kandidaten | 0 | 3 | 8 |

- **ADR-015**: Eine Funktion, die ein `DAG(...)` zurückgibt, ist ein DAG-Konstruktor des Repos.
  Gefundene Konstruktoren: `create_easy_dag`, `create_easy_cassandra_loading_dag`.
- **ADR-016**: `max_active_runs=1` ist selbst eine Cross-Run-Kante. Vorher hat der Scanner die
  Serialisierung, auf der dieser ganze Fall beruht, gar nicht gesehen.

Es bleibt eine Lücke: 90 der 345 DAGs haben keine `dag_id`, weil sie erst die aufrufende
Funktion einsetzt (`build_dag(dag_id=...)`). Unser Fall-DAG ist einer davon. Wir raten sie nicht.

## 6. Die ganze Organisation

`wikimedia/wikimedia_dags.csv`: 453 Zeilen, eine je (`dag_id`, Airflow-Instanz). 406 laufen
messbar, 280 stehen im Code, 233 in beidem. Für 249 kennen wir den geplanten Takt.

**30 DAGs haben eine mediane Laufzeit über ihrem geplanten Takt.** Bei 29 davon ist das kein
Drift: ohne `max_active_runs=1` und ohne Cross-Run-Signal laufen zwei Läufe schlicht nebeneinander.
Genau diese 29 wären die Fehlalarme eines Werkzeugs, das nur Laufzeit gegen Schedule hält. Übrig
bleibt einer mit einer Kante über die Zeitachse:

| dag_id | Takt | Median | Signal | Beleg |
|---|---|---|---|---|
| `mediarequest_hourly` | 3600 s | 6371 s | `external_task_sensor` | `main/dags/mediarequest/mediarequest_hourly_dag.py:46` |

Dazu die beiden Reconcile-DAGs, die in dieser Tabelle fehlen, weil ihre `dag_id` im Code nicht
steht (siehe Abschnitt 5).

## 7. Was dieser Fall nicht zeigt

- **Eine Organisation ist keine Stichprobe.** Wikimedia beweist nichts über den Markt. Der Fall
  zeigt, dass die Rechnung an echten Daten funktioniert und was sie findet, nicht wie häufig
  das ist.
- **Die Gauge hat Grenzen.** Zwei Läufe mit identischer Dauer auf die Millisekunde würden als
  einer zählen. Bei zehn DAGs meldet die Gauge mehr Wertwechsel, als ihr Takt erlaubt
  (`refine_api_requests_hourly`: 3360 in 30 Tagen bei stündlichem Takt). Warum, wissen wir
  nicht, und für diese DAGs rechnen wir kein λ.
- **Ob Wikimedia der Rückstand weh tut, wissen wir nicht.** 48 Minuten Verspätung bei stündlicher
  Reconciliation kann völlig in Ordnung sein. Wir sagen nicht, dass hier etwas kaputt ist. Wir
  sagen, dass niemand diese Zahl vorher ausgerechnet hat.
- **Wir haben Wikimedia nicht kontaktiert.** Alles hier stammt aus öffentlichen Quellen,
  read-only, jede Abfrage einmal und danach aus dem Cache.
