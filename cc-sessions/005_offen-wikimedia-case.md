# Session 005 — Der erste echte Fall: Wikimedia

**Phase 1 hat den Marktbeweis über öffentlichen Code nicht erbracht** (Session 003: 176
Risiko-Kandidaten von 51.426 DAGs, davon 78 % Beispielcode). Der Grund liegt nicht am Scanner,
sondern an der Grundgesamtheit: öffentliche Repos enthalten Lernmaterial und keine Laufzeiten.

Diese Session wechselt den Beweisort. **Wikimedia betreibt Airflow in Produktion, und beides
liegt offen: der DAG-Code und die gemessenen Laufzeiten.** Damit lässt sich λ zum ersten Mal an
einer echten Pipeline ausrechnen und gegen ihren tatsächlichen Takt halten.

Ziel ist **ein belegter Fall**, kein Produkt und keine Marktzahl.

## Die Quellen (in der Vorrecherche geprüft, Stand 2026-07-14)

**Code:** `https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags.git`, öffentlich,
klonbar ohne Login, rund 15 MB.

**Laufzeiten:** Wikimedias Grafana ist anonym abfragbar. Die Prometheus-Datenquelle mit den
Airflow-Metriken hat die UID `000000026`. Der Proxy antwortet ohne Authentifizierung:

```
https://grafana.wikimedia.org/api/datasources/proxy/uid/000000026/api/v1/query?query=<promql>
```

Vorhanden sind unter anderem `airflow_dagrun_duration` (Label `dag_id`,
`kubernetes_namespace`), `airflow_ti_finish` (Labels `dag_id`, `task_id`, `state`) und
`airflow_task_duration` (**ohne** `dag_id`/`task_id`, deshalb für uns wertlos). Es gibt 325
`dag_id`-Werte, davon 43 mit `hourly` im Namen, verteilt auf neun Instanzen (`airflow-main`,
`airflow-search`, `airflow-analytics-product`, `airflow-research`, `airflow-ml`, `airflow-wmde`,
`airflow-fr-tech`, `airflow-platform-eng`, `airflow-dev`).

**Fremde Infrastruktur, also sparsam:** read-only, Ergebnisse auf Disk cachen, keine engen
Polling-Schleifen. Jede Abfrage einmal, dann aus dem Cache arbeiten. Wir sind hier Gast.

## Der Fall, der die Session trägt

`search/dags/rdf_streaming_updater_reconcile.py`, Zeile 110 ff.:

```python
with create_easy_dag(
        dag_id=dag_id,
        default_args={'depends_on_past': True},
        schedule='@hourly',
        # We want hourly runs to be scheduled one ofter the other
        max_active_runs=1,
        catchup=True,
```

Stündlicher Takt, Abhängigkeit vom erfolgreichen Vorlauf, ein Lauf zur Zeit. Die gemessene
mittlere Dauer über 14 Tage (`avg_over_time(airflow_dagrun_duration{dag_id=~".*reconcile.*"}[14d])`):
`wdqs_streaming_updater_reconcile_hourly` etwa 60 bis 109 Minuten,
`wcqs_streaming_updater_reconcile_hourly` etwa 60 bis 106 Minuten.

Wenn diese Zahlen tragen, ist der Takt kürzer als die Taktgrenze, und das ist genau der Fall,
den `eigenlag` beschreibt. **Sie sind aber noch nicht belegt, sondern nur erhoben.** Das ist der
Auftrag dieser Session.

## Auftrag

### 1. `wikimedia/fetch.py` — Metriken holen und cachen

Ein Modul, das die Prometheus-Proxy-Abfragen kapselt und die Antworten roh nach
`data/wikimedia/<query-hash>.json` schreibt. Zweiter Lauf liest aus dem Cache. Fehler
(HTTP != 200, leere Serie) werden protokolliert, nicht geraten. Das ist Systemgrenze, hier
gehört Validierung hin.

Zu erheben, je DAG und je Instanz:

- `airflow_dagrun_duration` über ein Fenster von mindestens 30 Tagen, als Zeitreihe
  (`query_range`), **nicht** als einzelner Mittelwert. Wir brauchen die Verteilung, nicht die
  Zusammenfassung.
- `airflow_ti_finish` je `task_id`, um die Task-Ebene des Graphen mit Dauern zu füllen, falls
  die Metrik das hergibt. **Wenn nicht: sagen, nicht basteln.** Dann wird auf DAG-Ebene
  gerechnet und das im Report benannt.
- Alles, was tatsächlichen Rückstand zeigen könnte: Wartezeit der Runs, `queued`-Dauer,
  Scheduler-Verzug. Suche die vorhandenen Metriknamen über
  `/api/v1/label/__name__/values` und dokumentiere, was es gibt und was nicht.

**Die Gauge-Semantik ist eine offene Frage.** `airflow_dagrun_duration` kommt aus dem
StatsD-Exporter und ist eine Gauge, keine Histogramm-Verteilung. Was ein `avg_over_time` darüber
genau mittelt (jeden Scrape, also denselben Lauf mehrfach), muss geklärt werden, bevor eine Zahl
im Report steht. Wenn die Metrik keine belastbare Verteilung je Lauf hergibt, ist das ein
Ergebnis und kein Grund, sie trotzdem zu benutzen.

### 2. λ rechnen, mit unserem eigenen Kern

Für `wdqs_streaming_updater_reconcile_hourly` und `wcqs_streaming_updater_reconcile_hourly`:
Graph aus dem DAG-Code bauen (Tasks, Reihenfolge, Selbst-Kante aus `depends_on_past`), Dauern
aus den Metriken einsetzen, λ mit `eigenlag` (Session 004) ausrechnen, gegen den Takt von 3600
Sekunden halten, Drift je Lauf ausweisen.

Kein neuer Mathe-Code. Wenn der Kern etwas nicht kann, ist das ein Befund für den Report und
eine Spec für später, keine Ad-hoc-Erweiterung hier.

### 3. Der Scanner sieht Wikimedias DAGs nicht, und das gehört behoben

Der Scanner findet in diesem Repo **71 von 325 DAGs und null Cross-Run-Signale**, obwohl
`depends_on_past=True` mehrfach im Klartext dasteht (`search/dags/transfer_to_es.py:51`,
`search/dags/rdf_streaming_updater_reconcile.py:116`, `search/dags/embeddings.py:195`,
`search/dags/glent_weekly.py:112`). Grund: Wikimedia erzeugt DAGs über eine eigene
Wrapper-Funktion `create_easy_dag(...)`, `scanner/analyze.py` erkennt nur `DAG(...)` und `@dag`.
Dazu kommen 43 `unresolved_default_args`, weil die Defaults aus `wmf_airflow_common` importiert
werden.

**Das ist der wichtigste Befund aus der Recherche zu Session 003:** professionelle Umgebungen
kapseln ihre DAG-Erzeugung, und genau die sind für uns unsichtbar. Der Scanner ist blind
gegenüber der Zielgruppe, die das Produkt braucht, und das erklärt die Demo-Lastigkeit der
Marktzahl besser als jede andere Vermutung.

Auftrag: eine Regel, die eine lokale Wrapper-Funktion als DAG-Konstruktor erkennt, wenn im
selben Repo eine Funktion definiert ist, die ein `DAG(...)` zurückgibt oder als Contextmanager
liefert. Die Zuordnung bleibt DAG-scoped, geraten wird nicht. **Vorschlag ist ADR-015, aber die
Entscheidung gehört in den Plan der Session, nicht in diese Spec.** Wenn die Regel zu viele
Falsch-Positive erzeugt, ist ein sauber begründetes Nein besser als eine unscharfe Regel.

Danach: Wikimedia-Repo erneut scannen, Zahlen vorher/nachher nennen.

### 4. `wikimedia/case.md`

Der Fall zum Vorzeigen. Was der DAG tut, welchen Takt er hat, welche Kante ihn im Kreis führt,
was λ ist, was die Drift je Lauf ist, und wie das gemessen wurde. Jede Zahl mit der PromQL, die
sie erzeugt hat, und mit dem Permalink auf die Codezeile.

Dazu ein ehrlicher Abschnitt "Was dieser Fall nicht zeigt": eine Organisation ist keine
Stichprobe, die Gauge-Semantik hat Grenzen, und ob Wikimedia dieser Rückstand überhaupt weh tut,
wissen wir nicht.

## Was ausdrücklich nicht Auftrag ist

- **Kein Kontakt zu Wikimedia.** Keine Issues, keine Mails, keine Pull Requests, kein Post. Wir
  lesen öffentliche Daten, mehr nicht. Ob und wie David das später verwendet, entscheidet er.
- **Keine Last auf ihrer Infrastruktur.** Abfragen einmal, dann Cache.
- **Keine Verallgemeinerung auf den Markt.** Ein Fall ist ein Fall.

## Akzeptanz

- `data/wikimedia/` enthält die rohen Antworten, aus denen jede Zahl im Report stammt.
- λ und Drift für mindestens einen echten DAG, gerechnet mit `eigenlag`, mit Herleitung.
- Die Frage der Gauge-Semantik ist beantwortet, nicht umgangen.
- Der Scanner-Befund (71 von 325, null Signale) ist entweder behoben oder mit Begründung als
  offen dokumentiert.
- `wikimedia/case.md` liegt vor, mit PromQL und Permalinks zu jeder Zahl.
- Tests, `ruff`, `mypy` grün.

## Danach

Wenn der Fall trägt, ist er der Launch-Inhalt: eine bekannte Organisation, öffentlich
nachprüfbar, mit einer Zahl, die vorher niemand ausgerechnet hat. Wenn er nicht trägt, weil die
Metrik nicht hergibt, was sie zu versprechen scheint, ist auch das ein Ergebnis, und es ist
billiger, das jetzt zu wissen als nach Phase 2.
