# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 005 — Der Wikimedia-Fall (2026-07-14)

**λ ist zum ersten Mal an einer echten Pipeline gerechnet, mit gemessenen Laufzeiten, und der
Fall trägt.** Er trägt anders als die Vorrecherche annahm, und die Korrektur ist die bessere
Geschichte.

`wdqs_streaming_updater_reconcile_hourly` bei Wikimedia: stündlicher Takt (T = 3600 s),
`depends_on_past=True` und `max_active_runs=1`, also ein Kreis über die Zeitachse. Gemessen
über 398 Läufe in einem lückenfreien Fenster von 16,5 Tagen: **λ = 3598,4 s.** Der DAG driftet
nicht, er sitzt mit **1,6 Sekunden Reserve** auf seiner eigenen Taktgrenze. Der Preis dafür ist
eine Verspätung von **48 Minuten**, die nicht mehr wächst und nicht mehr verschwindet.

Der Fall liegt in `wikimedia/case.md`, mit PromQL und Permalink zu jeder Zahl.

### Was liegt

- `wikimedia/fetch.py` — Prometheus über Wikimedias Grafana-Proxy, Cache-first, Rate-Limit,
  Fehler nach `data/wikimedia/fetch_errors.jsonl`. 27 Requests im ganzen Lauf, read-only.
- `wikimedia/runs.py` — Läufe aus der Gauge rekonstruieren: ein Wertwechsel ist ein Lauf
  (ADR-017). Fenster ohne Metrik-Lücke, Statistik.
- `wikimedia/case.py` — Scan, Messung, λ über den Kern, Sweep über die Organisation.
  `python -m wikimedia.case` schreibt `data/wikimedia/case_numbers.json` und
  `wikimedia/wikimedia_dags.csv` (453 Zeilen, je DAG und Airflow-Instanz).
- `scanner/wrappers.py` — repo-eigene DAG-Konstruktoren (ADR-015).
- `scanner/analyze.py` — Signal G: `max_active_runs=1` (ADR-016).
- `scanner/schedule.py` — `period_seconds`: der Takt T wird aus dem Ausdruck gerechnet.

### Was verifiziert wurde

- `pytest`: **207 passed**. `ruff check`, `ruff format --check`, `mypy` grün.
- **Scanner auf Wikimedia:** 71 → **345 DAGs**, mit `dag_id` 58 → 255, mit Cross-Run-Signal
  0 → **68**, Risiko-Kandidaten 0 → **8**.
- **Gauge-Semantik belegt, nicht angenommen:** serverseitiges `changes()` liefert 397 Läufe,
  unsere Rekonstruktion 398. Mediane Laufzeit (3733,8 s) und medianer Abstand zweier Laufenden
  (3720 s) liegen 13,8 s auseinander, also unter der Scrape-Auflösung.
- **Der Sweep trennt echte von scheinbaren Problemen:** 30 DAGs haben eine mediane Laufzeit über
  ihrem Takt. **29 davon driften nicht**, weil ihre Läufe überlappen dürfen. Sie wären die
  Fehlalarme jedes Werkzeugs, das nur Laufzeit gegen Schedule hält.

## Hinweise für nächste Session

### Zwei Entscheidungen, die frühere kippen

- **ADR-016 ersetzt eine dokumentierte Festlegung.** `max_active_runs=1` stand in `signals.md`
  ausdrücklich unter "kein Cross-Run-Signal". Der Fall hat das widerlegt: ohne diese Kante hätte
  unser Modell für einen DAG, dessen Läufe rückenan liegen, "kein Kreis, kein λ" ergeben.
  `signals.md` und `math.md` sind korrigiert.
- **ADR-017** legt fest, dass Läufe aus der Gauge rekonstruiert und nicht über sie gemittelt
  werden. Die Vorrecherche hatte per `avg_over_time` "60 bis 109 Minuten" gelesen. Das war
  Scrape-Mittelung, nicht Lauf-Mittelung.

### Der Punkt, der als erstes entschieden werden muss

**Der Korpus-Scan aus Session 003 ist veraltet, und zwar zweifach.** Er kennt weder die
Konstruktoren (ADR-015) noch Signal G (ADR-016). Die Zahlen 51.426 DAGs / 1303 mit Cross-Run /
176 Risiko-Kandidaten stammen aus der alten Definition. Bei Wikimedia hat ADR-015 allein die
gefundenen DAGs verfünffacht. **Vor jeder öffentlichen Behauptung muss neu gescannt werden.**
Die Clones liegen noch (`data/repos/`, 74 GB), der Lauf ist wiederholbar, die Kosten sind
Rechenzeit, kein GitHub-Kontingent. Das ist der naheliegende Inhalt von Session 006.

Erwartung: die Marktzahl steigt deutlich, und der Anteil an Beispiel-Code sinkt, weil gekapselte
Repos jetzt sichtbar sind. Sicher ist das nicht, es ist eine Hypothese und muss gemessen werden.

### Weitere offene Punkte

- **90 der 345 Wikimedia-DAGs haben keine `dag_id`**, weil erst die aufrufende Funktion sie
  einsetzt (`build_dag(dag_id='wdqs_...')`). Unser eigener Fall-DAG ist einer davon, weshalb er
  in der Organisations-Tabelle fehlt. DAG-Generatoren mit Literal-Argumenten an der Aufrufstelle
  aufzulösen, wäre die Fortsetzung von ADR-015.
- **Zehn DAGs melden mehr Wertwechsel, als ihr Takt erlaubt** (`refine_api_requests_hourly`:
  3360 in 30 Tagen bei stündlichem Takt). Ursache unbekannt, λ wird für sie nicht gerechnet.
- **Die Task-Ebene gibt die Metrik nicht her.** Keine Dauer-Metrik für den Spark-Task,
  `airflow_task_duration` ohne `dag_id`/`task_id`, Sensoren im Reschedule-Modus melden Dauern
  nahe null. Gerechnet wird auf DAG-Ebene. Für die Task-Ebene braucht es die
  Airflow-Metadaten-DB, also einen Kunden.
- `numpy` wird im Kern erst bei Monte Carlo gebraucht, `pipx` ist noch nicht installiert.

### Was David entscheiden muss

1. **Session 006: Korpus neu scannen** (siehe oben) oder direkt Produkt bauen? Der Fall trägt
   auch ohne neue Marktzahl, aber die alte Zahl darf so nicht mehr behauptet werden.
2. **Der Fall ist Launch-Inhalt**, sobald David das will: eine bekannte Organisation, öffentlich
   nachprüfbar, mit einer Zahl (1,6 Sekunden Reserve, 48 Minuten Verspätung), die vorher niemand
   ausgerechnet hat. Kein Kontakt zu Wikimedia, keine Veröffentlichung, nichts nach außen ist
   passiert. Das entscheidet er.
