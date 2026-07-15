# Session 008 — Dauern-Schicht: aus Struktur-Aussagen werden Zeit-Aussagen

**Phase 2, Schritt nach dem Parser.** 007 liefert `Pipeline`-Strukturen mit uniformen Dauern; λ ist bisher eine Struktur-Aussage ("2 Tasks auf dem Kreis pro Periode"). Diese Session baut die Schicht, die echte Task-Dauern beschafft, und damit wird λ erstmals eine Zahl in Sekunden. Dazu der gezielte Sensor-Nachlauf aus der Abnahme 007a.

## Vorher lesen

- `wiki/math.md`, **Abschnitt 9 vollständig** — die Rückkopplungs-Grenze ist die zentrale semantische Falle dieser Session
- `wiki/architecture.md` (durations.py im Datenfluss), `wiki/decisions.md` ADR-006, ADR-019
- `eigenlag/parse_airflow.py` (`to_pipeline`-Schnittstelle, Task-Namespacing `dag_id.task_id`)
- Abnahme-Eintrag 005a und 007a in `wiki/log.md` (Ausreißer-Empfindlichkeit des Mittelwerts; die 14 offenen Sensorfälle)

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **Drei Quellen, eine Ausgabeform.** `eigenlag/durations.py` mit `from_metadata_db(url, dag_ids)`, `from_rest(base_url, auth)`, `assume(seconds)`. Alle liefern dieselbe Struktur: je Task `p50`, `p95`, `mean`, `n` (Stichprobengröße), `operator`, `is_sensor`. Eine Funktion `pick(stats, statistic)` macht daraus das `durations`-Mapping für `to_pipeline`.
2. **`sqlalchemy` wird optionales Extra** (`pip install eigenlag[db]`), Import lazy mit klarer Fehlermeldung. Der Kern bleibt bei null Pflicht-Dependencies. REST läuft über `urllib` aus der stdlib, nicht `httpx` — der einzige Grund für httpx wäre Komfort, und Komfort rechtfertigt keine Dependency (Regel 10).
3. **Welche Statistik in λ eingeht, entscheidet der Aufrufer, Default ist `mean` — und zwar dokumentiert warum:** für den asymptotischen Drift ist der Mittelwert die theoretisch richtige Größe (`wiki/math.md`), aber er ist ausreißer-empfindlich (der wcqs-Hänger: ein 4,6-Tage-Lauf verschiebt den Mittelwert um ~560 s, Abnahme 005a). Deshalb liefert die Schicht immer alle drei, und der Report in 009 zeigt λ auf `mean` **und** `p95` nebeneinander. Lognormal-Fit und Monte Carlo sind 009, nicht hier.
4. **Sensor-Dauern werden markiert, nicht herausgerechnet.** Die gemessene Dauer eines Sensors ist Wartezeit, und Wartezeit auf die Wanduhr ist genau die Rückkopplung aus `math.md` Abschnitt 9 — sie in ein Kreis-Gewicht zu stecken reproduziert die Zirkularität, die wir dort dokumentiert haben. Aus der Metadaten-DB lässt sich Warten nicht von Arbeiten trennen, also wird nicht so getan: Tasks mit Sensor-Operator bekommen `is_sensor=True`, und jede λ-Rechnung, in deren kritischem Kreis ein Sensor liegt, trägt eine Pflicht-Warnung ("Kreis enthält Wartezeit auf externe Ereignisse; λ kann überschätzt sein und ist keine harte Untergrenze mehr"). Das ist die ehrliche Grenze des Datenmodells, nicht ein Bug.
5. **Mindest-Stichprobe:** unter 5 erfolgreichen Läufen je Task gibt es keine Statistik, sondern eine Warnung und den `assume`-Fallback für diese Task. Eine p95 aus drei Werten ist eine Behauptung, keine Messung.

## Auftrag

### 1. Metadaten-DB (`from_metadata_db`)

Airflow legt Task-Läufe in `task_instance` ab. **Die Schema-Annahmen werden nicht aus dem Gedächtnis behauptet, sondern gegen ein echtes Airflow verifiziert** (siehe Verifikation). Erwartet werden: `dag_id`, `task_id`, `state`, `duration` (Sekunden), `operator`. Gefiltert wird auf `state = 'success'`. `task_id` enthält bei TaskGroups den Prefix (`gruppe.task`) — das muss zum Parser-Namespacing passen, ein Test pinnt es.

Aggregation in SQL (die DB kann das besser als Python bei Millionen Zeilen): p50/p95 über `percentile_cont` wo verfügbar, Fallback auf Python-Aggregation für SQLite. Zeitfenster einschränkbar (`--since`, Default 90 Tage), sonst mittelt man über Jahre alter Deployments.

### 2. REST-Fallback (`from_rest`)

Airflow-Stable-API: `GET /api/v1/dags/{dag_id}/dagRuns` + `.../taskInstances`, Basic Auth oder Token. Paginierung respektieren, Rate begrenzen (die API ist der Live-Scheduler, kein Data Warehouse — maximal 2 Requests/s), Abbruch nach konfigurierbarem Seiten-Deckel mit Warnung statt endlosem Crawl. urllib, kein httpx.

### 3. `assume(seconds)` und Mischbetrieb

`--assume-duration 300` gibt jeder Task 300 s. Mischbetrieb ist der Normalfall: DB liefert 40 von 45 Tasks, die 5 fehlenden (nie gelaufen, umbenannt) bekommen den Assume-Wert **mit Warnung je Task**. Fehlende Tasks stillschweigend auf 0 zu setzen wäre eine Lüge in Richtung "kein Problem".

### 4. Integration und der erste echte λ-in-Sekunden

`analyze(path, durations_source)` als dünne Kompositionsfunktion (noch keine CLI, das ist 009): parsen, Dauern heiraten, kondensieren, Howard, fertig ist das erste λ in Sekunden mit kritischem Kreis. Auf dem Flaggschiff-Fall `load_data_wikiviews` mit `assume`-Dauern durchspielen und ins Log — nicht als Zeit-Wahrheit (wir haben keine echten Dauern dieses Systems), sondern als End-to-End-Beleg, dass die Kette steht.

### 5. Sensor-Nachlauf (aus Abnahme 007a, gezielt statt Voll-Lauf)

Die 14 `sensor_not_modeled`-Fälle mit Grund "Ziel nicht im Parse-Satz": die betroffenen Repos als **ganze** Repos parsen (alle Python-Files, nicht nur die Kandidaten-Files). Je Fall dokumentieren: Ziel gefunden? Gleiches T? `delta/T` ganzzahlig? Ergebnis in drei Töpfe: modellierbar geworden / weiterhin nicht (mit Grund) / Ziel existiert nicht im Repo. **Wenn auch nur eine echte `periods > 1`-Kante dabei herauskommt, ist sie der erste Wildbahn-Beleg der ADR-006-Mechanik und wird mit Permalink im Log durchgerechnet.** Wenn nicht, ist auch das ein Ergebnis: die Mechanik bleibt test-belegt, und das steht dann so da.

## Verifikation (Kern-Akzeptanz: gegen ein echtes Airflow, nicht gegen Fixtures allein)

Die Schema-Annahmen (`task_instance`-Spalten, TaskGroup-Prefixe, `duration`-Semantik) sind der wackligste Teil, und Fixtures können sie nicht absichern, weil wir die Fixtures selbst nach denselben Annahmen bauen würden. Deshalb:

1. **Airflow standalone in einer separaten venv installieren** (`.venv-airflow/`, aktuelle stabile Version, Version im Log festhalten). Das ist Dev-Werkzeug, keine Package-Dependency — Regel 10 bleibt unverletzt.
2. Zwei kleine Test-DAGs einspielen (einer mit TaskGroup, einer mit `depends_on_past` und einem Sensor), per `airflow dags test` einige Läufe erzeugen.
3. `from_metadata_db` gegen die dabei entstandene DB laufen lassen: Kommen die Tasks mit Prefix richtig heraus, stimmen `operator` und `is_sensor`, ist `duration` plausibel? Output ins Log pasten.
4. `from_rest` gegen das lokale Airflow-API testen (standalone bringt den Webserver mit).
5. Dazu die normale Test-Suite: SQLite-Fixture-DB mit von Hand gebauten Zeilen und von Hand nachgerechneten p50/p95/mean-Pins, REST gegen gespeicherte JSON-Fixtures.

Wenn Airflow 3.x das Schema geändert hat und die Annahmen brechen: genau dafür ist dieser Schritt da. Beide Varianten unterstützen oder die Grenze dokumentieren — nicht raten.

## Akzeptanz

- `pytest` grün inkl. der von Hand gepinnten Statistik-Tests; `ruff`, `mypy` grün; Kern ohne Pflicht-Dependencies, `sqlalchemy` nur als Extra
- Schema-Verifikation gegen echtes Airflow standalone, Ausgabe im Log, Airflow-Version notiert
- Sensor-Markierung end-to-end: ein Fixture-Fall, in dessen kritischem Kreis ein Sensor liegt, erzeugt die Pflicht-Warnung
- Mindest-Stichproben-Regel und Mischbetrieb getestet
- Sensor-Nachlauf: alle 14 Fälle in einem der drei Töpfe, mit Beleg
- End-to-End-Kette `analyze()` am Flaggschiff-Fall im Log

## Explizit nicht in dieser Session

CLI, Report-Formatierung, Monte Carlo, What-if (alles 009), CI-Gate (010). Keine neuen Signal-Regeln. Und nicht versuchen, Sensor-Wartezeit von Sensor-Arbeit zu trennen — das gibt die Datenlage nicht her, und eine erfundene Trennung wäre schlimmer als die markierte Grenze.
