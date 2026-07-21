# Session-Log

Chronologisch. Neue Einträge unten anhängen. Jeder Eintrag nennt, was gemacht wurde, was gemessen wurde und was überrascht hat.

---

## 000 — Orchestrierung, Doku-Skelett, Prototyp-Verifikation (2026-07-13)

**Rolle:** Orchestrator. Kein Produktiv-Code geschrieben, das ist Absicht.

**Gemacht:**
- Projekt-Skelett: `CLAUDE.md`, `STATUS.md`, `README.md`, `wiki/`, `cc-sessions/`, git-Repo initialisiert.
- Wiki angelegt: `index`, `math`, `signals`, `architecture`, `positioning`, `roadmap`, `decisions`, `log`, `changelog`.
- Session-Specs 001 bis 004 geschrieben.

**Gemessen:**

Der Auftrag beschrieb `maxplus_pipeline.py` als vorhandenen, validierten Prototyp. Zu Sessionbeginn war die Datei auf keiner Maschine auffindbar (Suche über `/home/webse`, `/mnt/data`, `/tmp`). Die Referenzwerte waren damit unbelegt, und ADR-001 stand zunächst als offener Blocker im Wiki. David hat die Datei nachgereicht (`wiki/maxplus_pipeline.py`), zwei Zustellversuche kamen nicht an, der dritte über direktes Ablegen im Ordner hat funktioniert.

Prototyp ausgeführt, Ausgabe:

```
Critical Path eines Laufs (Latenz): 5.5 h
Nachhaltige Zykluszeit: lambda = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
(a) Retrain halbieren:            lambda = 3.60 h
(b) Quality-Gate asynchron:       lambda = 2.50 h
(c) Core-Job optimieren:          lambda = 3.85 h
```

λ zusätzlich von Hand nachgerechnet: Cross-Kante `monitor(k-1) → core(k)` speist den Intra-Pfad `core (1.1) + features (0.9) + retrain (1.6) + score (0.5) + monitor (0.3) = 4.4`, Kreislänge 1, Zyklusmittel 4.4. Alle Auftrags-Referenzwerte sind damit reproduziert **und** hergeleitet. ADR-001 aufgelöst.

**Was überrascht hat:**

1. **Der kritische Kreis ist nicht der, der im Auftrag steht.** Der Auftrag nennt `core → features → retrain → score → monitor`. Im kondensierten Graphen ist der Kreis aber die Selbst-Kante `monitor → monitor`, und die genannte Kette ist der aufgelöste Intra-Pfad dieses einen Segments. Beide Beschreibungen sind korrekt, meinen aber verschiedene Objekte. Wer nur eine davon im Report zeigt, produziert Verwirrung. Daraus wurde ADR-002.

2. **Der Prototyp kondensiert nicht wirklich.** `build_Abar` spannt die Matrix über alle acht Jobs auf, nicht nur über die drei Cross-Run-Quellen (`core`, `retrain`, `monitor`). Das Ergebnis stimmt trotzdem, weil Knoten ohne ausgehende Cross-Kante auf keinem Kreis liegen können und deshalb nichts beitragen. Für den Produktions-Code ist die echte Kondensation auf die Cross-Run-Knoten trotzdem richtig, weil Karp mit `O(V·E)` skaliert und V sonst unnötig die Task-Anzahl statt der Cross-Run-Knoten-Anzahl ist.

3. **Der Prototyp kennt keinen Perioden-Versatz.** `CROSS` ist eine Liste von Paaren, der Versatz ist implizit immer 1. Der im Auftrag geforderte Test-Case "Zwei-Perioden-Kreis via `execution_delta = 2 * Periode`" lässt sich mit dieser Datenstruktur nicht ausdrücken. Der Produktions-Datentyp braucht ein Tripel `(von, nach, versatz)`, und der Versatz muss ins Zyklusmittel eingehen (eine Kante mit Versatz n zählt als n Kanten). Das ist die erste echte Erweiterung über den Prototyp hinaus und steht so in Spec 004.

---

## 000a — Remote, GitHub-Limits gemessen (2026-07-14)

**Gemessen:** `gh api rate_limit` mit Davids Token. GitHub führt **zwei** Code-Such-Kontingente:

```
search      (/search/code, klassisch) : 30 req/min
code_search (neuer Endpunkt)          : 10 req/min
core        (/repos/..., Metadaten)   : 5000 req/h
```

Die im Auftrag genannten "30 req/min" gelten nur für den klassischen `/search/code`. Der neue `code_search`-Endpunkt liegt bei 10 und würde den Scan ohne Not verdreifachen. Spec 001 schreibt den Endpunkt jetzt explizit vor.

**Kosten:** keine. Die API ist für öffentliche Repos kostenlos, der Token authentifiziert nur und hebt die Limits (ohne Token: 60 req/h und gar keine Code-Search).

**Remote:** `github.com/webse-at/eigenlag` angelegt, `main` gepusht.

**Nächste Session:** 004 (Mathe-Kern). Begründung: der einzige Teil mit verifizierter Referenz, hängt an nichts, und er ist der eigentliche Produktwert. Der Scanner liefert Marketing-Zahlen; stimmt der Kern nicht, sind die Zahlen wertlos.

---

## 004 — Mathe-Kern: model.py, maxplus.py (2026-07-14)

**Gemacht:** `eigenlag/model.py` (Pipeline, CrossEdge, Toposort mit Zyklus-Erkennung), `eigenlag/maxplus.py` (Kondensation, Karp, Howard, Drift, Simulation, Critical Path), Tests daneben, `pyproject.toml` mit ruff/mypy/pytest-Konfiguration. Kein Parser, keine CLI, keine DB. Der Kern kennt weder Airflow noch dbt.

**Die eine offene Entscheidung** aus der Spec, `periods` im Zyklusmittel: `Summe der Gewichte / Summe der periods`, hergeleitet über die Zustandserweiterung. Steht als ADR-006 im Wiki.

**Gemessen — Kern gegen Prototyp, dieselbe Demo-Pipeline:**

```
===== PROTOTYP (wiki/maxplus_pipeline.py) =====
Critical Path eines Laufs (Latenz): 5.5 h
Nachhaltige Zykluszeit: lambda = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
(a) Retrain halbieren:      lambda = 3.60 h
(b) Quality-Gate asynchron: lambda = 2.50 h
(c) Core-Job optimieren:    lambda = 3.85 h

===== KERN (eigenlag/) =====
Critical Path eines Laufs: 5.5 h  (ingest -> dq -> core -> features -> retrain -> score -> reports)
lambda (Karp)   = 4.40 h
lambda (Howard) = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
```

Die drei What-if-Werte sind als Tests gepinnt und grün (3.60 / 2.50 / 3.85).

**Gemessen — Test-Suite, Lint, Typen:**

```
$ .venv/bin/python -m pytest -q
...................................                                      [100%]
35 passed in 0.03s

$ .venv/bin/ruff check . && .venv/bin/ruff format --check .
All checks passed!
5 files already formatted

$ .venv/bin/mypy eigenlag/
Success: no issues found in 5 source files
```

**Gemessen — Mutations-Test für `periods`.** Die 35 Tests waren beim ersten Lauf grün, was für sich genommen kein Beleg ist. Also gegengeprüft: in einer Kopie des Pakets `periods` in beiden Verfahren auf 1 gezwungen (Expansion abgeschaltet, `η · periods` durch `η` ersetzt). Ergebnis:

```
FAILED test_karp_and_howard_agree[mixed_periods]
FAILED test_karp_and_howard_agree[two_periods]
FAILED test_two_period_self_loop_halves_lambda
FAILED test_mixed_periods_cycle_mean_divides_by_the_sum_of_periods
4 failed, 31 passed
```

Genau die vier Perioden-Tests fallen und kein anderer. Damit ist die Frage aus STATUS ("wurde `periods` nur im Datentyp geführt?") beantwortet: nein, der Versatz kommt in der Rechnung an.

**Was überrascht hat:**

1. **Kein Kreis ist nicht dasselbe wie keine Cross-Kante.** Die Spec behandelt nur `cross == []`. Es gibt aber einen zweiten Fall: eine Cross-Kante `a(k-1) → b(k)`, bei der b nie wieder auf a zurückwirkt. Der kondensierte Graph hat dann Knoten, aber keinen Kreis, und λ ist genauso wenig definiert. Deshalb geben Karp und Howard `float | None` zurück statt `float` (ADR-007), und die Sonderbehandlung sitzt nicht im Aufrufer, wo sie jemand vergessen kann.

2. **Karp braucht keine starke Zusammenhangskomponente.** Karps Satz ist für stark zusammenhängende Graphen formuliert. Der Prototyp initialisiert `D[0][v] = 0` für **alle** v, was einer virtuellen Quelle mit Null-Kanten in jeden Knoten entspricht. Die erzeugt keinen Kreis, macht aber jeden Knoten erreichbar, und damit gilt die Formel auch für zerfallende Graphen. Der Test mit zwei disjunkten Kreisen (λ = max der beiden) belegt das, er wäre sonst der erste Kandidat für einen stillen Fehler gewesen.

3. **Parallele Cross-Kanten mit verschiedenem Versatz dürfen nicht zusammengefasst werden.** Bei der Kondensation liegt es nahe, pro Knotenpaar nur das maximale Gewicht zu behalten. Das ist falsch, sobald der Versatz unterschiedlich ist: Gewicht 6 bei Versatz 2 (Mittel 3) ist schlechter als Gewicht 4 bei Versatz 1 (Mittel 4), keine Kante dominiert die andere. Die kondensierte Matrix ist deshalb nach `(quelle, ziel, periods)` geschlüsselt, nicht nach `(quelle, ziel)`.

4. **`numpy` wurde nicht gebraucht.** Der Kern rechnet auf der kondensierten Matrix mit einstelliger Knotenzahl, Karp läuft in Millisekunden. Das Package hat damit **null** Laufzeit-Dependencies. Wenn Monte Carlo in Session 006 kommt, wird `numpy` wieder aktuell.

---

## 004a — Abnahme Mathe-Kern durch den Orchestrator (2026-07-14)

**Geprüft, nicht geglaubt.** Tests, `ruff` und `mypy` unabhängig nachgefahren: 35 passed, alles grün. Die acht Referenz-Pins gegen den Prototyp außerhalb der Test-Datei der Session nachgerechnet, alle deckungsgleich (λ = 4.40, Kreis `monitor → monitor` aufgelöst `core → features → retrain → score → monitor`, Critical Path 5.5, Drift 1.40 bei T = 3.0, What-ifs 3.60 / 2.50 / 3.85).

**Der offene Punkt aus STATUS war berechtigt und ist jetzt erledigt.** Die Session hatte selbst benannt, dass Howards Verbesserungsschritt keine Vorlage im Prototyp hat und sein einziger Beleg die Übereinstimmung mit Karp auf acht Fixtures ist. Das ist ein schwacher Beleg, weil beide Verfahren aus derselben Session stammen: ein gemeinsamer Denkfehler in der Perioden-Behandlung (ADR-006) hätte sich in beiden gleich ausgewirkt und wäre unentdeckt geblieben.

Gegenmaßnahme: ein **drittes, absichtlich stumpfes Verfahren** als Referenz. Alle einfachen Kreise aufzählen, je Kreis `Summe(w) / Summe(periods)`, Maximum. Das bildet ADR-006 direkt ab und enthält keinen Algorithmus, in dem sich ein Fehler verstecken kann. Ergebnis über 3000 Zufallsgraphen (bis 5 Knoten, mit Selbstkanten, Parallelkanten und gemischtem Versatz 1/2/3):

```
3000 Zufallsgraphen: 0 Abweichungen zwischen Brute-Force, Karp und Howard
kreislose Graphen darin: 529
```

Die 529 kreislosen Fälle sind wichtig: sie belegen, dass der `None`-Pfad aus ADR-007 wirklich durchlaufen wurde und nicht nur behauptet ist.

Der Kreuzvergleich liegt jetzt als `eigenlag/crosscheck_test.py` im Repo (1000 Graphen, 0.07 s), damit er bei jeder künftigen Änderung an Karp oder Howard mitläuft. **36 passed.** Die Restunsicherheit ist ehrlich zu benennen: getestet wurde bis 5 Knoten. Größere Graphen sind nicht abgedeckt, und der Vergleich auf echten geparsten DAGs bleibt wie in STATUS gefordert auf der Liste.

**ADR-007 (Abweichung von der Spec-Signatur) wird bestätigt.** Die Spec schrieb `-> float`, die Session liefert `-> float | None`. Die Begründung ist stichhaltig: der kreislose Fall tritt nicht nur bei `cross == []` auf, sondern auch bei einer Cross-Kante ohne Rückweg, und der Optional-Typ erzwingt die Behandlung beim Aufrufer statt sie zu vergessen. Die Spec war an dieser Stelle zu eng, nicht die Implementierung falsch.

---

## 001 — Scanner: Harvest-Schicht (2026-07-14)

**Gemacht:** `scanner/harvest.py` und `scanner/harvest_test.py`. Sechs Code-Search-Queries (fünf Airflow, eine dbt) gegen `/search/code`, Rohtreffer nach `data/hits.jsonl`, Metadaten aus dem `core`-Kontingent, Filterung, Ausgabe nach `data/candidates.jsonl` und `data/rejected.jsonl`. Kein Clone, kein AST, kein Report, das ist 002 und 003. Zweistufigkeit und Resume-Grenze stehen als ADR-008 im Wiki.

**Gemessen — der Lauf:**

```
=== Harvest-Ergebnis ===
Repos bewertet:   2095
Kandidaten:       1692
  davon Airflow:  1328
  davon dbt:      364
Verworfen:        403
  blocklist    251  (12.0 % der bewerteten Repos)
  size         152  (7.3 % der bewerteten Repos)
```

Die Akzeptanzschwelle der Spec (250 Airflow, 120 dbt) ist deutlich übertroffen, die Filter mussten nicht aufgeweicht werden. Vier der sechs Queries laufen in den 1000er-Deckel der Code-Search (`depends_on_past` allein meldet `total_count` 2284), die Stichprobe ist also nach oben abgeschnitten. Das gehört als Einschränkungssatz in `report.md`.

**Gemessen — Resume.** Der erste Lauf wurde nach der ersten Query absichtlich mit einem Kill abgebrochen. Stand danach: 1000 Zeilen in `hits.jsonl`, `harvest_state.json` mit `depends_on_past` auf `done: true`. Der Neustart begann mit

```
[fertig] depends_on_past language:python (aus vorigem Lauf)
[suche] wait_for_downstream language:python | Seite 1: 100 Treffer (total_count 1168)
```

und hat die 1000 bereits geholten Treffer nicht erneut gezogen.

**Gemessen — Drosselung.** Das `search`-Kontingent wurde im Lauf dreimal erschöpft. Der Scanner hat den 403 mit `x-ratelimit-remaining: 0` erkannt und bis zum Reset gewartet (`403 Rate-Limit, warte 41s`), ohne den Lauf abzubrechen. Ein einzelner 502 auf `/repos/uvasds-systems/run-airflow` landete strukturiert in `scan_errors.jsonl`.

**Gemessen — Tests, Lint, Typen:**

```
$ .venv/bin/python -m pytest -q
..............................................................           [100%]
62 passed in 0.09s

$ .venv/bin/ruff check . && .venv/bin/ruff format --check .
All checks passed!
9 files already formatted

$ .venv/bin/mypy
Success: no issues found in 9 source files
```

**Gemessen — Stichprobe, zehn Kandidaten zufällig gezogen und je Datei und Zeile aufgelöst.** Alle zehn sind belegbar, die Datei existiert und enthält den Suchbegriff an der genannten Zeile. Inhaltlich ist die Hälfte davon **kein** Cross-Run-Signal:

```
speaud/scripts-and-scraps      dags/tutorial.py:35        'depends_on_past': False,
Steve-YJ/pseudocon-8th-...     dags/sample.py:25          # 'wait_for_downstream': False,
navikt/team_familie_...        operators/kafka_operators.py:33   wait_for_downstream: bool = True,
mlmicozzi/AprendizajeMaquinaII dags/music_process.py:15   'depends_on_past': False,
antweiss/airflow-test          bashtest5.py:10            'depends_on_past': False,
cyrillettlin/DataEngineering   dags/us_accidents_bq_dag.py:81    execution_delta=timedelta(hours=3),
jyablonski/nba_elt_dbt         models/silver/fact/fact_reddit_posts.sql:22   {% if is_incremental() %}
SriGanesh78/dbt-core-cloud     models/bronze/bronze_orders.sql:8             {% if is_incremental() %}
dbtsurya123/dbt_cloud_st       models/staging/stg_orders.sql:12              {% if is_incremental() %}
oulrich-ops/dbt_certif_prepare models/base/stg_transactions.sql:17           {% if is_incremental() %}
```

**Was überrascht hat:**

1. **Die Stichprobe belegt ADR-004 empirisch, und zwar drastisch.** Drei der zehn Treffer sind `depends_on_past: False`, also die explizite Verneinung des Signals. Einer ist auskommentiert. Einer ist die Signatur einer selbstgeschriebenen Operator-Klasse (`wait_for_downstream: bool = True`), also nicht einmal ein DAG-Argument. Hätte der Scanner die Code-Search-Zahlen direkt als Marktzahl behauptet, wäre die Statistik beim ersten kritischen Leser gekippt. Die Trennung Kandidat/Signal ist damit keine Vorsichtsmaßnahme mehr, sondern gemessen notwendig. Für `report.md` heißt das: die Quote wird auf die AST-Ergebnisse aus 002 bezogen, nie auf die Kandidatenzahl.

2. **Die Filter `fork` und `archived` haben null Mal gegriffen.** Das sah zuerst nach totem Code aus. Nachgeprüft an zwanzig zufälligen Kandidaten über `/repos/...`: alle zwanzig sind `fork=false archived=false`. Die klassische Code-Search indiziert Forks nicht und liefert offenbar auch keine archivierten Repos aus. Der Filter bleibt trotzdem drin, weil er die Zusage der Spec einlöst und nichts kostet; die Aussage gehört aber in den Report, sonst liest sich "0 Forks verworfen" wie ein Filter-Fehler. Die Blocklist dagegen frisst 12 Prozent, das ist die Zahl, die David anfechten können muss (`rejected.jsonl`, Feld `reason`).

3. **Ein Paging-Bug wurde erst im echten Lauf sichtbar.** Bei `ExternalTaskSensor` lieferte Seite 7 nur 52 Treffer, die Query war erschöpft. Der Scanner hat trotzdem die Seiten 8, 9 und 10 abgefragt (je 0 Treffer), weil die Schleife nur gegen den Seiten-Deckel prüfte und nicht gegen das `done`-Flag. Drei verschwendete Requests gegen ein Kontingent von 30 pro Minute, keine falschen Daten. Behoben, indem die Fortschaltung als reine Funktion `advance(entry, n_items)` herausgezogen und mit einer Tabelle getestet wurde. Beleg am echten Endpunkt nach dem Fix: die Query bei Seite 7 wieder aufgesetzt, es kam genau eine Antwort mit 52 Treffern und dann Schluss. Der Fund ist ein Argument für die Regel, dass eine grüne Test-Suite Code-Korrektheit prüft und nicht Feature-Korrektheit: die HTTP-Schleife war zu keinem Zeitpunkt getestet, und genau dort saß der Fehler.

4. **Der 502 hat sich selbst repariert.** Das Repo fehlte nach dem Fehler in beiden Ausgabedateien und galt dem nächsten Lauf deshalb als offen. Er hat es nachgeholt, 1691 wurden 1692. Das war nicht geplant, sondern fällt aus der Zweistufigkeit heraus (ADR-008).

---

## 001a — Abnahme Scanner-Harvest durch den Orchestrator (2026-07-14)

**Zahlen unabhängig nachgerechnet**, direkt aus `data/`: 1692 Kandidaten (1328 Airflow, 364 dbt), 1692 davon eindeutig (keine Duplikate), 403 verworfen (251 Blocklist, 152 Größe), 2095 bewertet. Deckt sich exakt mit dem Bericht der Session. Akzeptanzschwelle der Spec (250 / 120) ohne Aufweichen der Filter übertroffen.

**Stichprobe gegengeprüft.** Sechs der zehn Belege selbst per `raw.githubusercontent.com` aufgelöst. Alle sechs stehen genau dort, wo die Session sie verortet hat:

```
speaud/scripts-and-scraps      .../dags/tutorial.py:35        'depends_on_past': False,
Steve-YJ/pseudocon-...         .../dags/sample.py:25          # 'wait_for_downstream': False,
navikt/team_familie_...        operators/kafka_operators.py:33   wait_for_downstream: bool = True,
mlmicozzi/AprendizajeMaquinaII .../dags/music_process.py:15   'depends_on_past': False,
antweiss/airflow-test          bashtest5.py:10                'depends_on_past': False,
cyrillettlin/DataEngineering_… .../us_accidents_bq_dag.py:81  execution_delta=timedelta(hours=3),
```

Der Befund der Session hält: rohe Code-Search-Treffer sind zur Hälfte keine Signale. ADR-004 ist damit gemessen und nicht nur begründet.

**Zwei Korrekturen am Befund der Session:**

1. **Der `navikt`-Fall ist kein Falsch-Positiv, sondern ein Falsch-Negativ mit Ansage.** Die Session hat ihn als "nur eine Funktionssignatur, kein DAG-Argument" abgetan. Tatsächlich ist es eine **Task-Factory**: die Funktion führt `depends_on_past: bool = True` und `wait_for_downstream: bool = True` als Defaults und gibt einen `KubernetesPodOperator` zurück (Zeile 83). Jeder Task aus dieser Factory trägt beide starken Signale. Das Signal ist echt, es steht nur in einem Helper-Modul, das kein DAG instanziiert. Spec 002 in ihrer bisherigen Form ("scanne DAG-Files, ordne DAG-scoped zu") hätte dieses Repo als signalfrei gemeldet. Daraus wurde **ADR-009**, und Spec 002 hat jetzt einen Abschnitt 4b: Factories werden erkannt, getrennt gezählt und **nicht** in die Hauptquote gemischt. Die Hauptquote ist damit ausdrücklich eine Untergrenze. Das ist die verteidigbare Fehlerrichtung, aber sie gehört in den Report.

2. **Die Belege im Session-Log sind gekürzt und damit nicht auflösbar.** Dort steht `dags/tutorial.py:35`, der echte Pfad ist `docker/sandbox/ubuntu-airflow/airflow/dags/tutorial.py`. Auch die Repo-Namen sind mit `...` abgeschnitten. Beim Nachprüfen liefen die ersten sechs `curl`-Aufrufe deshalb ins Leere, und ich musste zurück in `hits.jsonl`. Genau das verbietet Regel 6: ein Treffer, der sich nicht in dreißig Sekunden nachschlagen lässt, zählt nicht. In `scan_results.csv` und in `report.md` ist das kein Schönheitsfehler mehr, sondern ein Substanzfehler, weil der Beleg dort das Produkt ist. Spec 002 hält das jetzt fest.

**Übernommen für Session 003:** der 1000er-Deckel der Code-Search (vier von sechs Queries laufen hinein, `depends_on_past` meldet `total_count` 2284), die null Treffer der Filter `fork` und `archived`, und die zwölf Prozent Blocklist-Quote. Alle drei gehören in den Abschnitt "Was diese Zahlen nicht sagen".

---

## 002 — Scanner: AST-Analyse (2026-07-14)

**Gebaut:** `scanner/clone.py` (flache Clones mit Disk-Cache, 120 s Timeout, Fehler strukturiert protokolliert), `scanner/schedule.py` (Schedule-Klassifikation), `scanner/analyze.py` (DAG-Erkennung, DAG-Scoping, Signale A bis D und F, Task-Factories nach ADR-009), `scanner/analyze_dbt.py` (Signal E), dazu `scanner/fixtures/` mit zwei nachgebauten Repos, die jede Falle enthalten.

**Tests grün** (`pytest`, ganzes Repo):

```
.....................................................................    [100%]
141 passed in 0.21s
```

Davon 105 im Scanner (26 aus Session 001, 79 neu). `ruff check` und `ruff format --check` grün, `mypy` grün über 17 Files.

**Rauchtest über echte Repos.** Die Spec verlangt den großen Lauf erst in 003, aber eine Analyse, die nur gegen selbstgebaute Fixtures grün ist, beweist wenig. Deshalb 40 Kandidaten aus `candidates.jsonl` geklont und analysiert:

```
Repos:             40 (Clone-Fehler: 0)
DAGs:              352
Risiko-Kandidaten: 4 in 1 Repo
SyntaxError:       3
Schedules:         {'daily_or_slower': 217, 'none': 62, 'subdaily': 59, 'unknown': 14}
Signale:           {'external_task_sensor': 27, 'depends_on_past': 8}
Analyse-Fehler:    {'unresolved_default_args': 8, 'syntax_error': 3}
```

Belege der vier Risiko-Kandidaten, je von Hand im Clone nachgeschlagen:

```
DmitriiDenisov/airflow-lab | test_1_new      | timedelta(minutes=6)  | dags_examples/dag_bash_operator.py:8    'depends_on_past': True,
DmitriiDenisov/airflow-lab | long_bash       | timedelta(minutes=30) | dags_examples/dag_bash_operator_long.py:8
DmitriiDenisov/airflow-lab | branch_list_ex  | timedelta(minutes=2)  | dags_examples/branch_list_ex.py:33      'depends_on_past': True,
DmitriiDenisov/airflow-lab | example_branch… | timedelta(minutes=2)  | dags_examples/branch_operator_ex_3.py:33
```

Alle vier sind `depends_on_past: True` in einem `default_args`-Dict, das als Modul-Literal danebensteht und aufgelöst wird. Echte Treffer, keine Regex-Artefakte.

**Der ADR-009-Fall reproduziert sich exakt.** `navikt/team_familie_airflow_dags` (nicht in den ersten 40, separat geprüft) meldet zwei Factory-Signale, `operators/kafka_operators.py:32` und `:33` — genau die Zeilen, die die Abnahme von 001 von Hand gefunden hat. Das Repo hat 33 DAGs und **null** DAG-scoped Signale: ohne ADR-009 hätte der Scanner es als signalfrei gemeldet.

**Was überrascht hat:**

1. **`croniter` wird nicht gebraucht.** Die Spec hatte die Dependency erlaubt. Beim Hinschreiben der Herleitung fiel auf, dass die Klassifikation aus den expandierten Feldern direkt folgt: feuert ein Ausdruck mehr als einmal an einem Tag, liegt der kleinste Abstand zwangsläufig innerhalb des Tages, also unter 24 Stunden; feuert er höchstens einmal, ist der Abstand ein Vielfaches von 24 Stunden. Rund achtzig Zeilen stdlib statt einer Dependency, gegen die Tabelle aus `signals.md` getestet (ADR-010). Dafür kam `pyyaml` neu dazu, aber nur im Scanner-Extra: `dbt_project.yml` ist verschachteltes YAML mit `+`-Präfixen, das von Hand zu lesen wäre die falsche Sparsamkeit. Der Kern bleibt bei `dependencies = []`.

2. **`signals.md` widersprach sich selbst, und zwar an der Stelle, die die Marktzahl definiert.** Die Risiko-Definition zählte "A, B, C, D, E" auf, die Abstufung von Signal F nannte die `*_success`-Varianten aber ausdrücklich "harte Kanten" und schloss nur `prev_ds` und `prev_execution_date` aus. Ein Task, der auf `{{ prev_start_date_success }}` zugreift, wartet auf den erfolgreichen Vorlauf — das ist genau die Kante, die λ erzeugt. Die Aufzählung war die Verkürzung, die Abstufung die begründete Aussage. Wiki korrigiert, ADR-011.

3. **Zwei Fälle, in denen Auflösen erlaubt ist und Raten verboten bleibt.** Die Spec regelt `default_args` aus einem Modul-Dict-Literal. Im echten Lauf tauchte dasselbe Muster bei der `dag_id` auf: `mozilla/telemetry-airflow` schreibt `with models.DAG(dag_name, ...)` und setzt `dag_name = "copy_deduplicate"` drei Zeilen darüber. Ohne Auflösung steht in `scan_results.csv` eine leere `dag_id`, und ein Beleg ohne Namen ist ein halber Beleg (Regel 6). Modul-Ebene plus String-Literal wird deshalb aufgelöst, genau wie beim Dict. Eine `dag_id` aus einem f-String oder einer Schleifenvariable bleibt leer — dort wird nicht geraten.

4. **Der Clone-Cache hat die Test-Suite gesprengt, bevor er ihr geholfen hat.** `pytest` hat die Testfiles der geklonten Fremd-Repos eingesammelt und ist beim Import gestorben (fünf Collection-Fehler). `testpaths` und `norecursedirs` in `pyproject.toml` gepinnt. Derselbe Reflex hätte auch `ruff` getroffen, das respektiert aber `.gitignore` und hat `data/` von selbst ausgelassen.

5. **Ein Verzeichnis, das aussieht wie eine Datei.** `Swagatd/gcphandson` hat unter `target/compiled/` ein *Verzeichnis* namens `_analytics_models.yml`. `rglob("*.yml")` liefert es, `read_bytes()` wirft `IsADirectoryError`, der Lauf über 40 Repos stirbt bei Repo 6. Das ist die Sorte Fund, für die die Regel "fremde Repos sind Systemgrenze" existiert, und die keine noch so gute Fixture-Sammlung vorwegnimmt: sie fällt nur im echten Lauf an. Behoben, Regressionstest steht.

**Ambiguität wird protokolliert, nicht geraten.** Ein Operator ohne DAG-Bezug in einem File mit mehreren DAGs landet als `ambiguous_task` in den Fehlern, verankert am Operator-Aufruf (nicht am Signal-Keyword: ambig ist der Task, nicht das Argument). In einem File mit genau einem DAG wird er diesem zugeordnet und trägt `inferred=True`, damit die Konfidenz im Report trennbar bleibt.

## 003 — Scanner: Lauf, CSV, Report (2026-07-14)

**Gemacht:** `scanner/run.py` (resume-fähiger Lauf über die 1692 Kandidaten, Clone-SHA für die
Permalinks, Fehler nach `data/scan_errors.jsonl`), `scanner/report.py` (`scan_results.csv`,
`scan_factories.csv`, `scan_dbt.csv`, `report.md`), `task_count` pro DAG in `analyze.py`, beide
Stichproben nach `scan/sample_verification.md`. Artefakte liegen unter `scan/`, weil `data/`
ungetrackt ist.

**Gemessen (finaler Lauf, Lauf 3):** 1692 Repos, 1671 geklont, 21 Clone-Fehler (davon 20
Timeouts, die im zweiten Anlauf durchkamen). 317.706 Python-Files geparst, 7590 `SyntaxError`
protokolliert, kein Abbruch. **51.426 DAGs.** 1303 mit Cross-Run-Kante (2,5 %), 2543 sub-täglich
(4,9 %), **176 Risiko-Kandidaten (0,3 %) in 100 Repos.** dbt getrennt: 496 Repos mit
`dbt_project.yml`, 37.109 Models, davon 3369 mit echter Selbst-Kante (9,1 %).

**Was überrascht hat, und es ist der wichtigste Befund der Session:** 38.575 der 51.426 DAGs
(75 %) liegen in Beispiel-, Test- oder Doku-Pfaden, und unter den Risiko-Kandidaten sind es 138
von 176 (78 %). Airflows eigener Demo-DAG `example_branch_dop_operator_v3` trägt
`depends_on_past=True` bei `*/1 * * * *` und wird in jedes zweite Lern-Repo kopiert, ebenso
`test_dag.py` aus der Docker-Doku (`*/10 * * * *`). Die Risiko-Quote misst damit vor allem, wie
oft Airflow-Beispiele geforkt werden. Öffentliche Repos sind für diese Frage der falsche
Beweisort: dort liegt Lernmaterial, und Laufzeiten liegen dort ohnehin nicht.

**Drei Läufe, weil zwei Korrekturen nötig waren.** Lauf 1 (1840 s inkl. Klonen, 74 GB) diente
als Basis. Die Negativ-Suche deckte zwei Erkennungslücken bei Signal F auf (ADR-013), die
Positiv-Stichprobe einen echten Falsch-Positiv (`execution_delta=timedelta(hours=0)`, ADR-014).
Beide korrigiert, Lauf jeweils wiederholt (615 s ohne Klonen). Wirkung der Korrekturen:
Cross-Run 1335 → 1303, Risiko 182 → 176. Die alten Stände liegen als `data/scan_state_run1`
und `_run2`, nichts wurde überschrieben.

**Stichproben:** 10 Risiko-Kandidaten gegen den Quelltext geprüft, 0 Falsch-Positive. 10
signalfreie Kandidaten-Repos geprüft, 0 unbekannte Muster (alle schweigen wegen
`depends_on_past: False` oder auskommentiertem Code, wo ein Regex zehnmal angeschlagen hätte).

**`include_prior_dates` steht bei null DAGs, und das ist richtig.** Der Begriff kommt hunderte
Male vor, aber praktisch nur als `xcom_pull`-Parameter oder im Airflow-Quelltext. An einem
`ExternalTaskSensor` steht er in keinem einzigen der geklonten Repos. Signal D ist in freier
Wildbahn faktisch tot.

**Tests:** 140 passed, `ruff` und `mypy` grün.

## 003a — Recherche nach einem Beweisort mit echten Laufzeiten (2026-07-14)

Nach dem negativen Ausgang von Phase 1 die Frage: Wo gibt es Airflow-Laufzeiten, ohne eigenes
Netzwerk und ohne Tauschangebot? Drei Spuren geprüft, zwei tot, eine trägt.

**dbt-Artefakte auf GitHub: taugen nicht.** 1764 Repos haben ein `run_results.json` committet,
aber fast alle stammen aus `dbt docs generate`, also aus einem Compile-Lauf ohne Ausführung
(`coderxio/sagerx`: 166 Knoten, 0,6 s Gesamtzeit). Die wenigen echten Läufe unter `target/` sind
Spielzeugprojekte (`pietheinstrengholt/dbt-databricks-adventureworks`: 4 Knoten, 10 s). Dieselbe
Demo-Falle wie beim Code-Scan.

**GitHub-Issues und Stack Overflow: dünn.** `apache/airflow` hat 16 Issues zu
`depends_on_past` plus stuck/deadlock, das lauteste ist #29524 (Deadlock bei `max_active_runs`
plus `depends_on_past`). Stack Overflow liefert zu den naheliegenden Formulierungen fast nichts.
Der Schmerz ist nicht laut. Das ist selbst ein Befund.

**Wikimedia trägt.** Sie betreiben neun Airflow-Instanzen in Produktion, ihr Grafana ist anonym
abfragbar (Prometheus-Datenquelle `000000026`), und ihr DAG-Repo liegt offen auf
`gitlab.wikimedia.org`. 325 DAGs mit `dag_id`-Label, 43 davon stündlich.

**Der Fall:** `search/dags/rdf_streaming_updater_reconcile.py:110` erzeugt stündliche DAGs mit
`default_args={'depends_on_past': True}`, `max_active_runs=1`, `catchup=True`. Gemessene
mittlere Laufdauer über 14 Tage: `wdqs_streaming_updater_reconcile_hourly` 60 bis 109 Minuten,
`wcqs_streaming_updater_reconcile_hourly` 60 bis 106 Minuten. Stundentakt, Kreis über die
Zeitachse, Läufe im Mittel länger als eine Stunde. Genau die Konstellation der Kernthese, zum
ersten Mal an echten Zahlen. Noch erhoben, nicht belegt: die Gauge-Semantik von
`airflow_dagrun_duration` ist ungeklärt, das ist Auftrag von Session 005.

**Der unangenehme Nebenbefund, und er ist der wichtigere:** Unser Scanner findet in Wikimedias
Repo **71 von 325 DAGs und null Cross-Run-Signale**, obwohl `depends_on_past=True` dort mehrfach
im Klartext steht. Wikimedia erzeugt DAGs über eine eigene Wrapper-Funktion `create_easy_dag()`,
und `analyze.py` kennt nur `DAG(...)` und `@dag`. Professionelle Umgebungen kapseln ihre
DAG-Erzeugung, und genau die sind für uns unsichtbar. Das erklärt die Demo-Lastigkeit der
Marktzahl aus Session 003 besser als jede andere Vermutung, und es entwertet die 0,3 Prozent ein
zweites Mal, diesmal von der anderen Seite.

---

## Session 005 — Der Wikimedia-Fall (2026-07-14)

**Auftrag:** λ zum ersten Mal an einer echten Pipeline rechnen, mit echten Laufzeiten, und den Scanner-Blindfleck aus Session 003 beheben.

**Was gemacht wurde:** `wikimedia/fetch.py` (Prometheus über Wikimedias Grafana-Proxy, Cache auf Disk, Fehlerprotokoll), `wikimedia/runs.py` (Läufe aus der Gauge rekonstruieren), `wikimedia/case.py` (Scan, Messung, λ über den Kern, Sweep über die ganze Organisation), `scanner/wrappers.py` (ADR-015), Signal G in `analyze.py` (ADR-016), `period_seconds` in `schedule.py`. 67 neue Tests (207 im Repo).

**Was gemessen wurde, und was überrascht hat:**

**1. Der Fall trägt, aber anders als die Vorrecherche dachte.** Die Vorrecherche hatte aus `avg_over_time` "60 bis 109 Minuten mittlere Laufdauer" gelesen und daraus geschlossen, der Takt sei kürzer als die Laufzeit, die Verspätung wachse also unbegrenzt. Beides musste korrigiert werden. Die mittlere Laufzeit von `wdqs_streaming_updater_reconcile_hourly` beträgt **3598,4 s**, der Takt **3600 s**. Der DAG driftet **nicht**, er sitzt mit 1,6 Sekunden Reserve auf seiner Taktgrenze. Der Median (3733,8 s) liegt über dem Takt, der Mittelwert knapp darunter, und für die Drift zählt der Mittelwert.

**2. Die Bestätigung kam von einer Größe, die wir gar nicht gesucht hatten.** Die Läufe enden im Mittel alle 3599,5 s. Diese Zahl stammt aus Zeitstempeln, nicht aus Dauern, und sie sagt dasselbe: der DAG liefert genau einen Lauf pro Stunde, mehr geht nicht. Zwei unabhängige Größen, dieselbe Aussage.

**3. Der Preis ist sichtbar und dauerhaft: 48 Minuten.** `airflow_dagrun_schedule_delay` steht im Median bei 2880 s und wächst nicht mehr. Genau so sieht eine Pipeline an ihrer Taktgrenze aus: sie hält den Takt, aber dauerhaft eine Dreiviertelstunde zu spät. Das ist eine bessere Geschichte als "sie driftet", weil sie überprüfbar ist und weil sie erklärt, warum niemand es merkt.

**4. Warum sie trotz Median über dem Takt nicht wegdriftet, war der lehrreichste Teil.** Korrelation zwischen Verspätung beim Start und Laufzeit: **−0,504**. Je später ein Lauf startet, desto kürzer läuft er, weil die Sensoren auf die Daten der laufenden Stunde warten und diese bei einem verspäteten Start längst da sind. Der Sensor ist keine Bearbeitungszeit, sondern eine Synchronisation mit der Wanduhr, und er bricht den Kreis. Das ist eine echte Grenze des Max-Plus-Modells, sie steht jetzt in `math.md`, Abschnitt 9. Wer sie nicht kennt, bescheinigt gesunden Pipelines Drift.

**5. `max_active_runs=1` war als Nicht-Signal dokumentiert, und das war falsch.** `signals.md` führte es unter "kein Cross-Run-Signal": es begrenze die Nebenläufigkeit, nicht die Rekurrenz. Der Fall widerlegt das. Ohne diese Kante hätte unser eigenes Modell für den DAG "kein Kreis, kein λ" ergeben, für eine Pipeline, deren Läufe nachweislich rückenan liegen. Der Eigenwert kennt den Unterschied zwischen Daten- und Ressourcen-Abhängigkeit nicht, er sieht Kanten (ADR-016).

**6. Der Scanner-Blindfleck ist behoben, der Preis ist bekannt.** 71 → 345 DAGs, 0 → 68 mit Cross-Run-Signal, 0 → 8 Risiko-Kandidaten. Offen bleiben 90 DAGs ohne `dag_id`, weil erst die aufrufende Funktion sie einsetzt. Unser eigener Fall-DAG ist einer davon, und deshalb fehlt er in der Organisations-Tabelle. Geraten wird nicht (ADR-015).

**7. Der Sweep über alle 453 (DAG, Instanz)-Paare liefert das beste Produkt-Argument der Session.** 30 DAGs haben eine mediane Laufzeit über ihrem Takt. **29 davon driften nicht**, weil ihre Läufe überlappen dürfen. Genau diese 29 wären die Fehlalarme eines Werkzeugs, das nur Laufzeit gegen Schedule hält. Der Unterschied zwischen den 30 und dem einen ist der ganze Wert des Produkts, an echten Daten.

**Zwei Fehler in der eigenen Methode, gefunden durch unplausible Zahlen:**

- `maintenance_cleanup_airflow_db` zeigte 323 Läufe in 30 Tagen bei täglichem Takt. Ursache: derselbe `dag_id` läuft in **13 Airflow-Instanzen**, und `sum by (dag_id)` addierte sie. Der Sweep rechnet seitdem je Instanz.
- Die Lauf-Rekonstruktion führte anfangs die Serien mehrerer StatsD-Pods zusammen, **bevor** sie Wertwechsel zählte. Halten zwei Pods gleichzeitig verschiedene Werte, oszilliert die überlagerte Reihe, und jeder Sprung sähe aus wie ein Lauf. In diesen Daten hat es nicht zugeschlagen (die Pods lösten einander ab), die Falle ist trotzdem echt und jetzt getestet.

**Was offen bleibt:** Bei zehn DAGs meldet die Gauge mehr Wertwechsel, als ihr Takt erlaubt (`refine_api_requests_hourly`: 3360 in 30 Tagen bei stündlichem Takt). Ursache unbekannt, λ wird für sie nicht gerechnet.

**Last auf fremder Infrastruktur:** 27 Requests insgesamt, alles read-only, jede Antwort im Cache unter `data/wikimedia/cache/`. Kein Kontakt zu Wikimedia.

---

## 005a — Abnahme Wikimedia-Fall durch den Orchestrator (2026-07-14)

**Angenommen mit einer Korrektur an der Überschrift.** Die Messarbeit ist die beste des Projekts bisher. Die Schlussfolgerung, die daraus gezogen wird, ist es nicht.

**Nachgerechnet, wie λ = 3598,4 s entsteht.** `wikimedia/case.py`, `lambda_of()` baut eine Pipeline aus einem Knoten mit einer Selbst-Kante. Der Eigenwert eines solchen Graphen ist das Kantengewicht. In `data/wikimedia/case_numbers.json` steht es unverstellt: `dauer_s.mittel = 3598.4` und `lambda_s.mittel = 3598.4`, dasselbe für Median und p95. **λ ist die eingesetzte Dauer**, Kondensation und Howard sind hier eine Identitätsfunktion.

Das ist kein Fehler der Session: Wikimedia liefert `airflow_dagrun_duration`, also Dauern auf DAG-Ebene. Ohne Task-Dauern gibt es keinen gewichteten Task-Graphen, und das Modell fällt zwangsläufig auf einen Knoten zusammen. Die Session hat das einzig Mögliche getan, aber der Bericht verkauft es als etwas, das es nicht ist. Daraus wurde **ADR-017**.

**Zwei Korrekturen:**

1. **"1,6 Sekunden Reserve" muss weg.** Die Formulierung unterstellt eine knappe Marge, also einen Zufall. Die Session hat aber selbst die Korrelation −0,504 zwischen Startverspätung und Laufzeit gemessen und in `math.md` Abschnitt 9 geschrieben, dass "die gemessenen Dauern bereits das Ergebnis des eingeschwungenen Zustands" sind. Genau das heißt: Das System ist rückgekoppelt und pendelt sich dort ein, wo die mittlere Dauer ≈ T ist. Eine mittlere Dauer 1,6 s unter dem Takt ist **kein Balanceakt, sondern der Fixpunkt**, den ein selbststabilisierendes System einnehmen muss. Dieselbe Zahl kann nicht gleichzeitig ein Zirkularitäts-Beleg und eine knappe Marge sein.

2. **Der Fall belegt die These, nicht das Werkzeug.** Für DAGs, deren einzige Cross-Run-Kante Signal G (`max_active_runs=1`) ist, gilt λ = Makespan, und das ist die Laufzeit, die jedes Dashboard heute schon zeigt. Der Analyzer verdient sein Geld erst, wo der Kreis ein **Teilpfad** ist und λ < Makespan gilt. wdqs ist der am wenigsten aussagekräftige Falltyp für das Produkt.

**Die tragfähige Überschrift steht bereits in der Session, nur an der falschen Stelle:** 30 DAGs laufen im Median länger als ihr Takt, **29 davon driften nicht**, weil ihre Läufe überlappen dürfen. Das ist an echten Produktionsdaten gemessen und belegt, dass "Laufzeit über Takt" als Diagnose wertlos ist. Das trägt den Fall.

**Bestätigt, gegen meine eigene frühere Festlegung:**

- **ADR-016 ist richtig, `signals.md` war falsch.** Ich hatte `max_active_runs=1` ausdrücklich unter "kein Cross-Run-Signal" geführt, mit der Begründung, es begrenze die Nebenläufigkeit und nicht die Rekurrenz. Für den Eigenwert ist diese Unterscheidung bedeutungslos: `Ende(k−1) ≤ Start(k)` ist eine Kante über die Zeitachse, und eine Kante ist eine Kante. Hätte die Session meiner Festlegung gehorcht, hätte das Modell für eine Pipeline, deren Läufe nachweislich rückenan liegen, "kein Kreis, kein λ" gemeldet. Die Session hat richtig widersprochen.
- **ADR-015** (Funktionen, die ein `DAG(...)` zurückgeben, sind DAG-Konstruktoren): bei Wikimedia allein verfünffacht das die gefundenen DAGs (71 → 345). Der Verzicht auf Transitivität ist richtig begründet, und der Preis (90 DAGs ohne `dag_id`) wird protokolliert statt geraten.

**Nebenbefund, den die Session nicht nennt:** λ auf dem Mittelwert ist ausreißer-empfindlich. Bei `wcqs` verzerrt ein einzelner hängender Lauf von 400.132 s (4,6 Tage) den Mittelwert um rund 560 s bei 712 Läufen. Für den asymptotischen Drift ist der Mittelwert die richtige Statistik, aber ein hängender Lauf vergiftet ihn. Gehört benannt, nicht geglättet.

**Der Re-Scan (Session 006) ist zwingend.** Die Zahlen aus 003 (51.426 DAGs, 176 Risiko-Kandidaten) kennen weder die Konstruktoren (ADR-015) noch Signal G (ADR-016). Bei Wikimedia allein hat ADR-015 die gefundenen DAGs verfünffacht. Vor jeder öffentlichen Behauptung wird neu gescannt.

---

## Session 006 — Re-Scan mit Zwei-Klassen-Risiko, Fall-Korrektur (2026-07-14)

**Auftrag:** Korpus unter der Definition nach ADR-015/016 neu scannen, Signal G als eigene Klasse ausweisen (ADR-018), `case.md` nach ADR-017 korrigieren. Spec: `cc-sessions/006_offen-rescan-korrektur.md`.

**Was gemacht wurde:** ADR-018 (zwei Risiko-Klassen), `report.py` um `sig_g_max_active_runs`, `risk_candidate_g_only`, `dag_id_missing`, Vorher/Nachher-Tabelle und Offenlegungs-Absätze erweitert (Tests zuerst, 6 neue rot → grün). Voller Re-Scan über die 1692 gecachten Clones, State versioniert neu unter `data/scan_state_v2/` (Clones unangetastet, Regel 8). Artefakte nach `scan/v2/`, die alten bleiben liegen. `wikimedia/case.md` nach ADR-017 umgebaut. `signals.md` auf die Zwei-Klassen-Definition gebracht.

**Der Lauf:** 1692 Repos in 1193 s, 10 Worker, 0 Abbrüche, kein Neu-Klonen (Log: `data/scan_run_v2.log`). Etwa doppelt so lang wie der 003-Lauf (615 s), das ist der Preis des ADR-015-Vorlaufs, der jedes Repo zweimal parst.

**Was gemessen wurde, und was überrascht hat:**

**1. Die Kern-Quote ist nicht nur gleich groß, sie ist mengen-identisch.** 1303 Cross-Run-DAGs und 176 Kern-Kandidaten sind exakt dieselben Zeilen wie in 003 (per Key `repo, file, dag_id, lineno` verglichen, beide Richtungen leer). Gegenprobe: das neue `report.py` auf dem alten 003-State reproduziert 51.426 / 1303 / 176 mit 0 G-only. Die Definitionsgleichheit ist damit in beide Richtungen belegt, nicht behauptet.

**2. Die 005-Hypothese "die Marktzahl steigt deutlich" ist widerlegt.** ADR-015 bringt im öffentlichen Korpus +422 DAG-Zeilen und −59 (netto +363, also +0,7 %), davon 401 ohne `dag_id`, und kein einziges neues Signal. 330 der 422 stammen aus einem einzigen Repo (`mik-laj/airflow-api-clients`): ein generierter OpenAPI-Client, dessen Modellklasse `DAG` heißt und dessen Test-Methode `make_instance` sie zurückgibt — die repo-weite Namensauflösung von ADR-015 macht daraus DAG-Scopes. Die −59 sind die Schablonen in Konstruktor-Rümpfen (überwiegend vendorierter Airflow-Quellcode), die 003 fälschlich als DAGs zählte. Der öffentliche Korpus kapselt seine DAG-Erzeugung schlicht kaum; Konstruktoren sind ein Muster professioneller Umgebungen (Wikimedia: Verfünffachung), und genau die fehlen in der Code-Search-Stichprobe. Das ist die ehrlichere Pointe von ADR-015 und ein weiterer Beleg für den Demo-Vorbehalt des Reports.

**3. Die neue G-Klasse ist groß: 3529 DAGs mit `max_active_runs=1` (6,8 %), davon 473 sub-tägliche G-only-Kandidaten in 159 Repos.** Hätte man G stillschweigend ins STRONG-Set gemischt, wäre die "Risiko-Quote" von 176 auf 649 gesprungen — mit Kandidaten, die jedes Laufzeit-Dashboard genauso findet. Genau das verhindert ADR-018: 176 bleibt die Launch-Zahl, 473 steht als eigene Zeile daneben, mit dem Satz "Laufzeit-Monitoring reicht dort" im Report, bevor ihn ein Kritiker sagt. Nur 10 der 176 Kern-Kandidaten tragen zusätzlich G.

**4. Stichproben: 0 Falsch-Positive, 0 Falsch-Negative, ein Beleg-Fehler gefunden und behoben.** Drei 10er-Stichproben (Kern, G-only, signalfrei; `random.Random(6)`, ein DAG je Repo) in `scan/v2/sample_verification.md`. Der wertvollste Fund der Negativ-Stichprobe: `shaqbari/de6_3th_day6_naver` setzt `execution_delta=timedelta(hours=0)` — der ADR-014-Fall in freier Wildbahn, korrekt kein Signal. Der Beleg-Fehler: `njuxc/PYAM` trägt Dateien mit `#` im Namen, der Permalink verlor dadurch den Zeilen-Anker (Regel-6-Verstoß). `permalink()` encodiert seit dieser Session die Pfade, mit Test.

**5. dbt exakt übernommen, nicht neu definiert.** `analyze_dbt.py` unverändert, `scan/v2/scan_dbt.csv` byte-identisch mit 003 (per `diff` geprüft), im Report als übernommen gekennzeichnet.

**6. Fall-Korrektur nach ADR-017 umgesetzt.** `case.md` führt jetzt mit dem Sweep (30 DAGs über Takt, 29 driften nicht), "1,6 Sekunden Reserve" ist überall gestrichen und durch den Fixpunkt-Befund ersetzt (Korrelation −0,504, mittlere Dauer ≈ T ist der eingeschwungene Zustand), λ = Laufdauer auf DAG-Ebene steht explizit im Dokument, der wcqs-Ausreißer (400.132 s verschieben den Mittelwert um ~560 s bei 712 Läufen) hat seinen eigenen Absatz. Messwerte unverändert. Log-Eintrag 005 blieb unangetastet: die Richtigstellung steht bereits vollständig in 005a, ein Nachtrag hätte nur dupliziert.

**Kleine Deltas am Rand, der Vollständigkeit halber:** `unresolved_default_args` 5641 → 5629, `ambiguous_task` 428 → 434, Repos mit DAG 1286 → 1287. Alles Folgen von ADR-015 (mehr bzw. andere Scopes ändern die Zuordnung einzelner Fundstellen); keine dieser Größen geht in eine Quote ein.

**Verifiziert:** `pytest` 214 passed, `ruff check`, `ruff format --check`, `mypy` über `eigenlag/`, `scanner/`, `wikimedia/` grün. Kein GitHub-Request im ganzen Lauf, alles aus dem Clone-Cache.

---

## 006a — Abnahme Re-Scan durch den Orchestrator (2026-07-14)

**Abgenommen.** Alle Kopfzahlen unabhängig aus beiden CSVs nachgerechnet, ohne den Report zu lesen:

```
003 (alt): DAGs=51426 crossrun=1303 kern=176 g_only=0
006 (neu): DAGs=51789 crossrun=1303 kern=176 g_only=473 g_repos=159
Kern nur in alt: 0   nur in neu: 0        (Mengen-Identität bestätigt)
DAG-Delta: +422 / −59 (netto 363), Top-Quelle: mik-laj/airflow-api-clients (330, alle signalfrei)
```

Deckungsgleich mit dem Session-Bericht, Zeile für Zeile. 214 Tests, ruff, mypy selbst nachgefahren. Die "Reserve"-Formulierung ist aus `case.md` vollständig raus, der Sweep steht als Überschrift, die Definitionsänderung ist im Report offengelegt.

**Der wertvollste ungeplante Fund des Reports:** 75,2 % der DAGs (und 78,4 % der Kern-Kandidaten) liegen in Beispiel-/Tutorial-Pfaden oder tragen `example_`-IDs — der Airflow-eigene Lehr-DAG `example_branch_dop_operator_v3` mit `depends_on_past=True` bei Minutentakt wird in jedes zweite Lern-Repo kopiert. Keine Spec hat diese Auswertung verlangt. Sie ist der wichtigste Vorbehalt gegen jede Marktaussage aus diesem Korpus und gehört in jede öffentliche Verwendung der Zahlen.

**Zwei Bereinigungen durch den Orchestrator:**

1. **ADR-Nummern-Kollision behoben, und sie war mein Fehler.** Bei der Abnahme von 005 habe ich das These-ADR als "ADR-017" nummeriert, ohne zu prüfen, dass Session 005 die Nummer bereits für die Gauge-Rekonstruktion vergeben hatte. Das These-ADR heißt jetzt **ADR-019** (mit Kollisions-Notiz im ADR selbst), die lebenden Verweise in `index.md`, `roadmap.md` und `case.md` sind umgestellt. Log-Einträge und abgeschlossene Specs bleiben als Historie unverändert.

2. **Die Vorher/Nachher-Zeile im Report war zu glatt.** "ADR-015 findet Konstruktor-DAGs" als Ursache für +363 unterschlägt, dass 330 davon **keine DAGs sind**: ein generierter OpenAPI-Client, dessen Modellklasse `DAG` heißt. Zähler unberührt (alle signalfrei), Nenner um 0,6 % verwässert. Steht jetzt als eigener Absatz im Report, mit dem Import-Check als ADR-Kandidat für die nächste Scanner-Session — nicht in dieser umgesetzt, die Zaun-Regel aus Spec 006 gilt.

**Bewertung der Kernbotschaft:** Dass die Kern-Quote mengen-identisch blieb, ist kein enttäuschendes, sondern das bestmögliche Ergebnis: Es belegt, dass die Zahl 176 gegen zwei Definitions-Erweiterungen stabil ist, und die Gegenprobe (neues `report.py` auf altem State reproduziert exakt die alten Zahlen) schließt aus, dass sich zwei Fehler gegenseitig aufheben. Die 473 G-only-Kandidaten daneben zeigen, was passiert wäre, hätte man G still ins STRONG-Set gemischt: eine Quote von 649, zu drei Vierteln aus Fällen, die ein Laufzeit-Dashboard beantwortet. Die Zwei-Klassen-Entscheidung (ADR-018) hat sich am ersten echten Lauf bewährt.

**Phase 1 ist damit abgeschlossen.** `scan/v2/` ist der zitierfähige Stand. Nächster Schritt nach Roadmap: 007, der Airflow-Parser — mit der Auflage aus ADR-019, früh einen echten Teilpfad-Fall (λ < Makespan) zu finden, denn das ist der Falltyp, den kein Dashboard beantwortet.

---

## Session 007 — Airflow-Parser: vom DAG-File zur Pipeline (2026-07-14)

**Was gebaut wurde:** `eigenlag/parse_airflow.py` — eigene AST-Extraktion (nicht die des Scanners, Vorentscheid 2 der Spec): `ParsedDag` mit Tasks, Intra-Kanten, `ParsedCrossEdge` (Kante plus Signal-Art, Datei, Zeile) und `Warning_` für alles Erkannte-aber-nicht-Modellierte. Dazu `to_pipeline(dags, durations=1.0)`, `parse_source`/`parse_files`/`parse_path`, und der Umzug `scanner/schedule.py` → `eigenlag/schedule.py` (Scanner importiert seither aus dem Package; Abhängigkeits-Richtung Produkt ← Scanner).

**Tests zuerst, rot gesehen:** Die Übersetzungstabelle stand komplett als Tests, bevor das Modul existierte — Beleg: `ModuleNotFoundError: No module named 'eigenlag.parse_airflow'` beim ersten Lauf, danach 39 passed. Zwei Korpus-Funde wurden ebenfalls erst als rote Tests fixiert (Ausgabe `2 failed, 39 passed`), dann behoben: (1) `ClassDef`-Rümpfe wurden nicht durchlaufen — Sensoren in unittest-Methoden vendorter Airflow-Testfiles fehlten; (2) bei `depends_on_past` **und** `wait_for_downstream` am selben DAG meldete der Parser nur B — erkannt sind aber beide Signale, die Selbstkante trägt jetzt A.

**Import-Beleg von Anfang an:** Ein File, dessen `DAG` nicht nachweislich aus `airflow` importiert ist (positiver Beleg gefordert), wird nicht geparst, Warnung `dag_not_airflow`. Im Korpus feuerte das 3-mal, alle drei berechtigt: `pythontool.conf.airflow` (vendorte Airflow-Kopie, 2 Files) und `dag_parser.dynamic.dag_context` (eigener Wrapper). Das ist der 330-Zeilen-Fehler aus 006, im Produkt von Tag eins verhindert.

**Korpus-Lauf** (`scanner/parse_corpus.py`, 288 s, Artefakte in `scan/007_parse/`), über die DAG-Files der 176 Kern- und 473 G-only-Kandidaten aus `scan/v2/`:

- **Parse-Quote:** 626 Files aus 207 Repos (626 von 626 vorhandenen Kandidaten-Files, 0 fehlend), **0 Syntax-Fehler**, 4892 DAGs, davon 3583 mit statischer `dag_id` (73,2 %). 646 von 649 Kandidaten-Zeilen aus dem Scan wiedergefunden; die 3 fehlenden sind exakt die 3 `dag_not_airflow`-Files.
- **Warnungs-Verteilung** (je Vorkommen): `file:ambiguous_task` 1472, `dynamic_task_id` 822, `unresolved_default_args` 336, `edge_dropped` 177, `task_dag_inferred` 54, `max_active_runs` (erkannt, DAG ohne Tasks) 45, `task_mapping` 28, `sensor_not_modeled` 27, `sensor_dynamic_offset` 7, `prev_run_success` 6, `dag_not_airflow` 3, `wait_for_downstream` (default_args ohne tragende Task) 1. Die hohen ersten beiden Posten stammen überwiegend aus vendorten Airflow-Testfiles (viele DAGs pro File, f-String-IDs in Schleifen) — jede Warnung trägt Datei und Zeile (`warnings.jsonl`).
- **Konsistenz Parser ↔ Scanner:** 3 Abweichungen, alle drei die dokumentierte Modell-Differenz Import-Beleg (der Scanner-seitige Import-Check ist laut Spec explizit ausgeklammert, ADR-Kandidat aus 006a). Signal-Arten je DAG sonst deckungsgleich; auf den Fixtures per `scanner/parse_consistency_test.py` dauerhaft gepinnt.
- **Kreuzvergleich (offen aus Abnahme 004): Karp = Howard auf allen 4836 kondensierten Graphen, 4827 davon (≤ 8 Knoten) zusätzlich per Brute-Force bestätigt. 0 Abweichungen.** 768 Graphen mit Kreis, größter Graph 15 Knoten. 56 Komponenten nicht rechenbar (`pipeline_invalid`): vendorte Airflow-Testfiles, die absichtlich zyklische Intra-Graphen bauen (Airflows eigene Cycle-Detection-Tests) — Systemgrenze, geloggt, Lauf lief weiter.
- **Sensor-Kanten (C): im ganzen Kandidaten-Korpus war keine einzige statisch modellierbar.** Gründe (34 Fälle): 14× Ziel-DAG nicht im Parse-Satz, 7× `execution_date_fn`, 7× `delta/T` nicht ganzzahlig (meist Minuten-Versatz bei Stunden-Takt), 5× verschiedene Takte, 1× Versatz nicht auflösbar. Vorbehalt: der Parse-Satz waren die Kandidaten-Files, nicht ganze Repos — Ziele in Nicht-Kandidaten-Files desselben Repos zählen als "nicht im Parse-Satz". Die `periods > 1`-Mechanik (ADR-006) ist damit durch Tests belegt, nicht durch den Korpus.

**Teilpfad-Jagd (Auflage aus ADR-019): der Produkt-Fall existiert in öffentlichem Code.** 129 eindeutige Kern-Kandidaten-DAGs in 77 Repos haben λ < Critical Path bei uniformen Dauern (`scan/007_parse/teilpfad.csv`, je Zeile mit Permalink): 131 Zeilen mit λ = 1 (einzelne `depends_on_past`-Selbstkante), 4 mit λ = 2. Alle λ-Aussagen sind Struktur-Aussagen in Einheiten "Tasks auf dem Kreis pro Periode" (uniforme Dauer 1.0), keine Zeit-Aussagen.

**Der durchgerechnete Fall** (`supratim94336/SparkifyDataPipelineWithAirflow`, `airflow/dags/udacity_dag.py`, das Udacity-Sparkify-Projekt): `default_args` setzt `wait_for_downstream: True` bei explizitem `depends_on_past: False` (Zeilen 26–27). B an `Load_songplays_fact_table` erzeugt die Kante `load_dimensions(k−1) → Load_songplays_fact_table(k)`; der kondensierte kritische Kreis ist die Selbst-Kante `load_dimensions → load_dimensions` mit Gewicht 2, aufgelöst (ADR-002) der Pfad `Load_songplays_fact_table → load_dimensions`. **λ = 2,0 bei Critical Path 6,0** (Begin → Stage → Load_songplays → load_dimensions → Quality → Stop). Von Hand: jede Periode muss die zwei Kreis-Tasks aufnehmen, also 2 Task-Slots, während ein Einzellauf 6 braucht. Ein Laufzeit-Dashboard zeigt 6 und alarmiert bei T < 6; die echte Taktgrenze ist 2. Vorbehalt: uniforme Dauern, Struktur-Aussage. Permalink in `teilpfad.csv`.

**Was überrascht hat:** (1) Kein einziger statisch modellierbarer `ExternalTaskSensor` im Kandidaten-Korpus — der ADR-006-Fall ist in öffentlichem Code praktisch nicht auffindbar, vermutlich weil Multi-DAG-Systeme mit sauberen Versätzen in privaten Repos leben. (2) Die Teilpfad-Fälle sind fast ausschließlich λ = 1: `depends_on_past` an einzelnen Tasks dominiert; echte Mehr-Task-Kreise (B-Kaskaden) sind selten, aber es gibt sie. (3) Die vendorten Airflow-Kopien im Korpus verzerren jede File-Ebene-Statistik; für Marktzahlen bleibt `scan/v2/` maßgeblich, `scan/007_parse/` ist ein Technik-, kein Markt-Artefakt.

**Verifiziert:** `pytest` 256 passed (Ausgabe im Abschluss-Commit-Kontext), `ruff check`, `ruff format --check`, `mypy` grün — Beleg unten im Abschlussblock. Kern weiterhin ohne Laufzeit-Dependencies (`dependencies = []` unverändert).

---

## 007a — Abnahme Airflow-Parser durch den Orchestrator (2026-07-14)

**Abgenommen.** 256 Tests, ruff, mypy (33 Files) unabhängig nachgefahren. `stats.json` gegen den Session-Bericht geprüft: 626 Files, 207 Repos, 4892 DAGs, 646/649 Kandidaten-Zeilen wiedergefunden, Karp = Howard auf 4836 Graphen (4827 zusätzlich Brute-Force), `mismatches.jsonl` enthält exakt die 3 gewollten Import-Beleg-Fälle. Die Teilpfad-CSV nachgezählt: 135 Zeilen = 129 eindeutige (repo, dag)-Paare in 77 Repos — die scheinbare Diskrepanz im Bericht sind Datei-Duplikate im selben Repo, kein Fehler.

**End-to-end selbst nachgerechnet, mit dem Produkt-Code statt den Session-Skripten:** `parse_path → to_pipeline → condense → howard` auf `Gleb01548/russian_wiki_view_db / load_data_wikiviews` ergibt λ = 2.0, Critical Path = 3.0 bei uniformen Dauern, deckungsgleich mit `teilpfad.csv`. Die TaskGroup-Auflösung (Prefix-Namespace) und die `wait_for_downstream`-Expansion (Selbst-Kante plus direkte Downstreams) sind an diesem Fall sichtbar korrekt.

**Eine inhaltliche Korrektur: der falsche Vorzeige-Fall.** Das Session-Log rechnet `udac_example_dag` durch — das ist Udacity-Kurscode, und zwar erkennbar (`udac`, "Sparkify"). Unser eigener Report sagt, dass 78 % der Kern-Kandidaten Anschauungsmaterial sind; dann darf der eine durchgerechnete Launch-Fall nicht ausgerechnet aus dieser Menge kommen. Die Marker-Prüfung über die vier Mehr-Task-Fälle ergibt: drei sind **kein** Beispiel-Code — `Gleb01548/russian_wiki_view_db` (zwei DAGs, stündlicher Wikipedia-Views-Download nach Postgres/ClickHouse, ein echtes Hobby-Produktionssystem samt Telegram-Alerting) und `scotthavens/docker-airflow / hrrr_retrevial` (Wetterdaten-Abholung). **Der Vorzeige-Fall für Launch-Material ist `load_data_wikiviews`**, jetzt zusätzlich vom Orchestrator end-to-end verifiziert. `udac` bleibt in der Liste, wird aber nicht mehr als Flaggschiff erzählt.

**Antworten auf die zwei Prüffragen aus STATUS:**

1. **Trägt der λ=1-lastige Katalog den Launch?** Als Zählung ja, als Demo-Material nur über die drei Nicht-Beispiel-Fälle. Die ehrliche Erzählung hat drei Stufen: (a) die These trägt der Wikimedia-Sweep (29 von 30 driften nicht), (b) die Struktur-Zählung trägt der Korpus (129 DAGs, deren Taktgrenze strukturell unter ihrem Critical Path liegt, 4 davon mit Mehr-Task-Kreis), (c) die Fähigkeits-Demo trägt `load_data_wikiviews`. Dass Mehr-Task-Kreise in öffentlichem Code selten sind (4 von 4892), wird mitgenannt — es stützt sogar das Produkt-Argument: die interessanten Fälle liegen in privaten Produktions-Repos, nicht auf GitHub, und genau dort läuft das CLI.
2. **Teurerer Lauf für die 14 "Ziel nicht im Parse-Satz"-Sensorfälle?** Ja, aber gezielt: nur die betroffenen Repos als Ganzes parsen, kein Voll-Korpus-Lauf. Eine einzige echte `periods > 1`-Kante in freier Wildbahn wäre die beste Demonstration der ADR-006-Mechanik, die bisher nur durch Tests belegt ist. Das wird ein Zusatz-Abschnitt in Spec 008, keine eigene Session.

**Sauber gelöst, hervorhebenswert:** Die zwei Parser-Bugs aus dem Konsistenz-Vergleich (ClassDef-Rümpfe, A+B-Gleichzeitigkeit) wurden erst als rote Tests fixiert und dann behoben — genau die Reihenfolge, die CLAUDE.md verlangt. Und die Sensor-Ernüchterung (keine einzige statisch modellierbare C-Kante im Kandidaten-Korpus, alle 34 Fälle mit konkretem Grund) wurde gemeldet statt schöngerechnet.

---

## Session 008 — Dauern-Schicht: aus Struktur-Aussagen werden Zeit-Aussagen (2026-07-15)

**Auftrag:** `eigenlag/durations.py` (drei Quellen, eine Ausgabeform), `analyze()` als Kompositionsfunktion, Schema-Verifikation gegen ein echtes Airflow, Sensor-Nachlauf der 14 offenen Fälle aus 007a. Spec: `cc-sessions/008_offen-dauern-schicht.md`.

**Was gebaut wurde:**

- `eigenlag/durations.py`: `TaskStats` (p50/p95/mean/n/operator, `is_sensor` aus dem Operator-Namen — `"Sensor" in operator`, damit auch `DateTimeSensorAsync` zählt), `assume(seconds)` (n=0 sagt ehrlich: Annahme, keine Statistik), `pick(stats, statistic)` und `resolve(tasks, stats, statistic, fallback, min_n=5)`. Mischbetrieb ist der Normalfall: fehlende Tasks und Tasks unter der Mindest-Stichprobe bekommen den Assume-Wert **je Task mit Warnung** (`dauer_angenommen` bzw. `stichprobe_zu_klein`); ohne Fallback wirft `resolve` statt still 0 zu setzen. Perzentile per linearer Interpolation, identisch mit `percentile_cont` — Pins von Hand: `[10,20,30,40,50] → p50=30, p95=48`.
- `from_metadata_db(url, dag_ids, since_days=90)`: sqlalchemy lazy importiert (Extra `eigenlag[db]`, Kern bleibt bei `dependencies = []`). Auf PostgreSQL aggregiert die DB selbst (`percentile_cont`), sonst holt die Query die Dauern und Python aggregiert (SQLite kennt kein `percentile_cont`). Zeitfenster Default 90 Tage.
- `from_rest(base_url, auth, dag_ids, ..., api_version="v2")`: urllib, kein httpx (Vorentscheid 2). Paginierung über `total_entries`, maximal 2 Requests/s (`min_interval_s=0.5`), Abbruch nach `max_pages` je DAG mit Warnung `rest_seiten_deckel`.
- `eigenlag/analyze.py`: `analyze(path, stats, statistic="mean", fallback=None)` — parsen, Dauern heiraten (`resolve`), kondensieren, Howard. Liegt ein Task mit `is_sensor=True` auf dem aufgelösten kritischen Kreis, trägt das Ergebnis die Pflicht-Warnung `sensor_im_kritischen_kreis` ("Kreis enthaelt Wartezeit auf externe Ereignisse; Lambda kann ueberschaetzt sein und ist keine harte Untergrenze mehr") — markieren statt herausrechnen (Vorentscheid 4, `math.md` Abschnitt 9). Nebenbei: `_node_name` im Parser ist jetzt öffentlich (`node_name`), statt die Namensbildung in `analyze` zu duplizieren.

**Tests zuerst, rot gesehen:** `ModuleNotFoundError: No module named 'eigenlag.durations'` beim ersten Lauf der 13 Duration-Tests, danach grün; dasselbe für die 5 `analyze`-Tests (`No module named 'eigenlag.analyze'`). Statistik-Pins von Hand im Test-Docstring hergeleitet, TaskGroup-Namespacing DB ↔ Parser als eigener Pin-Test (`etl.grp.load` aus beiden Welten identisch).

**Schema-Verifikation gegen echtes Airflow (Kern-Akzeptanz).** `uv venv .venv-airflow --python 3.12` (Python 3.12.13; Airflow trägt das 3.14 des Servers nicht), `apache-airflow==3.3.0` mit offiziellem Constraints-File. Zwei Test-DAGs (als Beleg kopiert nach `scan/008_sensor/testfall_*.py`): `testfall_gruppe` (TaskGroup `grp` mit BashOperator) und `testfall_dop_sensor` (`TimeDeltaSensor` + BashOperator mit `depends_on_past=True`). Je 6 Läufe per `airflow dags test` mit verschiedenen Logical Dates. `from_metadata_db` gegen die entstandene DB:

```
testfall_dop_sensor.arbeit          n= 6 mean=  1.186 p50=  1.181 p95=  1.199 operator=BashOperator is_sensor=False
testfall_dop_sensor.warten          n= 6 mean=  1.791 p50=  1.788 p95=  1.823 operator=TimeDeltaSensor is_sensor=True
testfall_gruppe.ende                n= 6 mean=  1.042 p50=  1.040 p95=  1.046 operator=BashOperator is_sensor=False
testfall_gruppe.grp.laden           n= 6 mean=  2.045 p50=  2.046 p95=  2.048 operator=BashOperator is_sensor=False
testfall_gruppe.start               n= 6 mean=  3.041 p50=  3.041 p95=  3.067 operator=BashOperator is_sensor=False
```

Alle Annahmen halten: Spalten `dag_id, task_id, state, duration, operator, start_date` existieren in Airflow 3.3.0, `duration` ist Sekunden (sleep 2 → 2.046), `task_id` trägt den TaskGroup-Prefix (`grp.laden`), `operator` ist der Klassenname. Zwei Befunde am Rand: (1) `start_date` liegt in SQLite als **naiver UTC-String ohne Offset** (`2026-07-15 06:45:46.635753`) — der Fenster-Vergleich bindet deshalb exakt dieses Format (Kommentar im Code). (2) Die jeweils erste Task eines Laufs trägt ~2 s Overhead in `duration` (sleep 1 → konstant 3.04) — das ist Airflows Messung, nicht unsere; für uns zählt nur: Einheit Sekunden, plausible Werte.

**Airflow 3 hat die REST-Annahmen der Spec gebrochen — genau dafür war der Schritt da.** Gegen den laufenden `airflow api-server` (3.3.0): `GET /api/v1/...` → **404** (der v1-Pfad ist entfernt), Basic Auth gegen v2 → **401** (Airflow 3 verlangt JWT-Bearer-Token via `POST /auth/token`). Die Antwort-Felder von `/api/v2/dags/{dag_id}/dagRuns/~/taskInstances` sind strukturgleich mit v1 (`task_id`, `state`, `duration`, `operator`). Konsequenz umgesetzt statt geraten: `api_version`-Parameter, Default `"v2"` (Token-Auth), `"v1"` für Airflow 2 (dort auch Basic-Tupel) — beide Pfade getestet. `from_rest` live gegen das lokale API liefert **exakt dieselben fünf Zeilen** wie `from_metadata_db` oben — zwei unabhängige Quellen, gleiche Aggregation.

**End-to-End, zweimal:**

1. *Echte Dauern:* `analyze(testfall_dop_sensor.py, stats=DB)` → **λ = 1,186 s** (Selbstkante `arbeit → arbeit`, das ist exakt `mean(arbeit)`), Critical Path 2,977 s (`warten → arbeit`). Ein Teilpfad-Fall in Sekunden, aus einer echten Metadaten-DB, durch die komplette Kette. Der Sensor liegt nicht auf dem Kreis → korrekt keine Warnung; der Fixture-Fall mit Sensor **auf** dem Kreis (Pflicht-Warnung) ist `eigenlag/analyze_test.py`.
2. *Flaggschiff mit Assume:* `analyze(load_data_wikiviews, assume(300))` → **λ = 600 s** gegen Critical Path 900 s, kritischer Kreis kondensiert `load_data → load_data`, aufgelöst `сheck_data → load_data`, 8 Warnungen `dauer_angenommen` (eine je Task, wie gefordert). Das ist die 007a-Struktur-Aussage (λ = 2, CP = 3) × 300 s — kein Zeit-Beweis (wir haben keine echten Dauern dieses Systems), aber der Beleg, dass die Kette steht.

**Sensor-Nachlauf (die 14 Fälle aus 007a, ganze Repos als Parse-Satz).** `scanner/sensor_followup.py`, Artefakt `scan/008_sensor/nachlauf.csv`, jede Zeile mit Permalink. Ergebnis der drei Töpfe: **1 modellierbar geworden, 11 weiterhin nicht (mit Grund), 2 Ziel existiert nicht im Repo.**

- *Modellierbar:* `IanRJ19/PEDE5_Airflow` — `ets_master` (`* * * * *`, T=60 s) wartet mit `execution_delta=timedelta(minutes=1)` auf `ets_slave.load` (gleicher Takt, Nachbar-File). Kante `ets_slave.load(k−1) → ets_master.sensor(k)`, periods=1 — die erste in freier Wildbahn modellierte Sensor-Kante. Durchgerechnet: sie erzeugt **keinen Kreis** (kein Rückweg von master nach slave), λ ist auf diesem Paar korrekt "nicht anwendbar" (ADR-007). Kein `periods > 1`.
- *Weiterhin nicht (11):* 5× `delta/T` nicht ganzzahlig (Minuten-Versatz bei größerem Takt), 2× verschiedene Takte, 1× Takt nicht bestimmbar, 1× `external_task_id` nicht statisch, 1× Ziel-Task nicht im Ziel-DAG (`An4PDM`: Ziel-Task `imprime_1` existiert im Ziel-DAG nicht — erst der Ganz-Repo-Parse macht diesen präziseren Grund sichtbar), 1× **Selbst-Referenz** (unten).
- *Ziel nicht im Repo (2):* beide `amitmentos/Big_Data_Project` — `silver_to_gold_etl` kommt im ganzen Repo nur als `external_dag_id`-Referenz und in Demo-Strings vor, kein DAG trägt diese ID (per grep belegt).

**Damit gilt: kein `periods > 1`-Wildbahn-Beleg — die ADR-006-Mechanik bleibt test-belegt, und das steht dann so da** (die Spec nennt genau dieses Ergebnis als gültig).

**Befund für den Orchestrator — Selbst-Referenz-Sensor:** `bhatiadeepak0805/OmniRoute_Project_Group_4`, `DAG_Codes/dag_2.py:480` — ein `ExternalTaskSensor` mit `external_dag_id` = **eigener** DAG (`dag2_batch_pipeline_harsh`, Schedule `0 0,5 * * *`), wartet auf den eigenen Task `vehicle_registry_silver` von vor 5 Stunden. Das Ziel existiert also (es ist der Quell-DAG selbst), aber der Parser modelliert nur Fremd-DAG-Sensoren (`d is not draft` in `_sensor_edges`). Semantisch ist das eine echte Selbst-Rekurrenz-Kandidatin; hier zusätzlich durch den unregelmäßigen Takt (5 h/19 h-Lücken) nicht im Ein-Perioden-Modell darstellbar. Keine neue Signal-Regel in dieser Session (Spec-Zaun) — ADR-Kandidat.

**Verifiziert:**

```
pytest: 274 passed (18 neue)
ruff check: All checks passed!  |  ruff format --check: 38 files already formatted
mypy: Success: no issues found in 38 source files
```

Kern weiterhin ohne Pflicht-Dependencies (`dependencies = []` unverändert); `sqlalchemy` als neues Extra `db` und in `dev` (für die SQLite-Fixture-Tests). `.venv-airflow/` ist Dev-Werkzeug, gitignored, wegwerfbar; die Verifikation ist hier und in `scan/008_sensor/` dokumentiert.

---

## 008a — Abnahme Dauern-Schicht durch den Orchestrator (2026-07-15)

**Abgenommen.** 274 Tests, ruff, mypy (38 Files) unabhängig nachgefahren. Flaggschiff end-to-end selbst gerechnet: `analyze(load_data_wikiviews, fallback=assume(300))` → λ = 600.0 s, CP = 900.0 s, 8 `dauer_angenommen`-Warnungen — deckungsgleich mit dem Session-Bericht und konsistent mit der 007-Struktur (2.0/3.0 × 300). Nachlauf-Töpfe aus `nachlauf.csv` nachgezählt: 14 = 11 weiterhin nicht + 1 modellierbar + 2 Ziel nicht im Repo.

**Der Airflow-3-Fund ist der Verifikationsschritt bei der Arbeit.** Die Spec hat die Schema-Verifikation gegen ein echtes Airflow genau deshalb verlangt, weil Fixtures dieselben Annahmen enthalten hätten wie der Code. Ergebnis: DB-Annahmen halten, aber `/api/v1` ist weg und Basic Auth abgeschafft — ohne den Schritt wäre der REST-Pfad gegen eine tote API gebaut worden und der Fehler erst beim ersten echten Nutzer aufgefallen. Dass DB- und REST-Pfad auf denselben 12 echten Läufen identische Statistik liefern, ist der richtige Beleg-Typ: zwei Wege, eine Wahrheit.

**Der Selbst-Referenz-Fall wurde zu ADR-021.** Die Session hat bei der Handprüfung erkannt, dass `bhatiadeepak0805` kein fehlendes Ziel ist, sondern ein Sensor auf den eigenen DAG (`execution_delta = 1 × T` bei T = 5 h), und die Entscheidung korrekt eskaliert statt selbst zu modellieren. Entscheidung: wird modelliert (die sauberste Sensor-Kante überhaupt, kein Merge-Problem, keine T-Frage), Umsetzung in 009. Details im ADR.

**Offen und in Spec 009 aufgenommen:** der Postgres-Aggregationspfad (`percentile_cont`) lief nur gegen Pins, nicht gegen ein echtes Postgres. Kommt in 009 als Wegwerf-Container (`docker run postgres`), nicht gegen die Dev-DB eines anderen Projekts.

**Entscheidung zum 009-Schnitt (die offene Orchestrator-Frage aus STATUS):** **Airflow-only-CLI zuerst, dbt-Parser vertagt bis nach dem Feedback-Meilenstein.** Begründung: (1) Die Zwischenbewertung nach Phase 1 (positioning.md) empfiehlt, das Tool nach 009 an 2–3 echte Teams zu bringen — das CLI ist das gating Artefakt, jede Woche dbt-Parser davor verzögert den einzigen Test, der die Marktfrage beantworten kann. (2) dbt-λ ist strukturell fast immer der λ=1-Fall (Selbst-Kante des inkrementellen Models); der Produktwert dort ist die Abdeckungs-Erzählung, nicht die Rechnung, und die blockiert nichts. (3) Wenn das Feedback sagt, dass die dbt-Nutzer der eigentliche Markt sind, bauen wir den Parser mit dem Wissen, welchen Report sie brauchen — statt vorher zu raten. Roadmap entsprechend umgestellt: 008b (dbt) hängt jetzt am Feedback-Meilenstein nach 009.

---

## Session 009 — CLI `eigenlag analyze`: der Report ist das Produkt (2026-07-15)

**Auftrag:** `eigenlag/cli.py` (argparse, Entry-Point, Exit-Codes 0/1/2), `eigenlag/report.py` (deutscher Report + `--json`), `eigenlag/montecarlo.py` (stdlib), ADR-021 im Parser, vier Verifikations-Läufe, pipx-Rauchtest. Spec: `cc-sessions/009_offen-cli.md`.

**Was gebaut wurde (alles Tests-zuerst, rot gesehen):**

- **ADR-021 umgesetzt:** `_sensor_edges` behandelt `external_dag_id == eigene dag_id` als Selbst-Referenz — Kante `CrossEdge(ziel_task, sensor_task, n)` im eigenen Namespace, T-Gleichheits- und Merge-Frage entfallen per Konstruktion. 4 neue Tests (n=1, n=2, kein Vielfaches → Warnung wie gehabt, Namespace-Pin über `to_pipeline`); erster Lauf: 3 failed wie erwartet (der Warnungs-Fall war schon vorher Warnung).
- **`eigenlag/montecarlo.py`:** analytischer Lognormal-Fit aus den Aggregaten (`mu = ln p50`, `sigma = (ln p95 − ln p50) / 1.6449`), Sampling per `random.lognormvariate`, Auswertung per `statistics.quantiles(n=100, method="inclusive")`. Kondensation läuft **pro Sample** neu; der Pfadwechsel-Test pinnt das mit einer Fixture, in der zwei Intra-Pfade konkurrieren (p lognormal-breit p50=90/p95=200, q konstant 100): Sample-Median exakt der q-Pfad (115), p95 der p-Pfad (~215) — eine einmal gebaute Matrix bliebe bei 115 hängen. Tasks mit n < 5 (inkl. `assume`, n=0) samplen konstant und stehen im Report. Seed fest (Default 20260715): derselbe Aufruf liefert dieselben Zahlen, per Test gepinnt.
- **`eigenlag/report.py`:** `compose()` baut ein dict mit stabilen Keys (das CI-Gate in 010 liest genau diese Felder), `render()` macht daraus den deutschen Text — eine Quelle, Text und `--json` können nicht auseinanderlaufen. Reihenfolge wie in der Spec: Kopf, Urteil (stabil / an der Grenze mit Abschnitt-9-Hinweis / instabil mit Drift und Zeit bis 1 h Rückstand; kein Kreis → „nicht anwendbar: keine Cross-Run-Kante", per Test gepinnt dass nirgends „Lambda = 0" steht), Kreis doppelt (ADR-002, je Segment mit Signal und Datei:Zeile aus `ParsedCrossEdge`), Monte Carlo (Pendel-Satz wenn λ_p95 > T > λ_p50), What-if-Ranking (Standard-Szenarien automatisch: jede Kreis-Task halbiert, jede Cross-Kante entfernt, sortiert; „bringt exakt null"-Satz), Pflicht-Warnblock (nie abschaltbar; Sensor-im-Kreis-Text, F-Divergenz-Erklärung nach ADR-020, angenommene/dünne Dauern, nicht modellierte Kanten mit Datei:Zeile), Modellgrenzen-Fußzeile (Untergrenze, Retries/Pools, Makespan).
- **`eigenlag/cli.py`:** `eigenlag analyze PFAD` mit exakt der Spec-Flag-Liste, Entry-Point über `[project.scripts]`. Quellen-Mischung wie 008 (DB/REST liefert, `--assume-duration` füllt je Task mit Warnung); ohne jede Quelle Abbruch mit Erklärung. `--dag-id` zieht transitiv die DAGs mit, auf die Sensor-Kanten zeigen (sonst wäre die Pipeline nicht baubar). DB-URLs werden im Report-Kopf passwort-geschwärzt. Exit-Codes per Test gepinnt: 0 auch bei instabil, 1 Bedienfehler (auch unbekannter What-if-Task), 2 kein analysierbarer DAG (mit Warnungs-Liste auf stderr).
- **`analyze_result()`** als Schwester von `analyze()` (Refactor): die CLI parst zuerst (Filter, dag_ids für die Metadaten-Query) und analysiert den gefilterten Satz.

**Messvorbehalt Monte Carlo (Vorentscheidung 2): 1000 Samples auf der Demo-Pipeline in 0,05 s** — Faktor 100 unter der 5-s-Schwelle, `numpy` bleibt draußen, `dependencies = []` unverändert. Beleg unten (Lauf 1).

**007-Graph-Check neu gelaufen** (Konsistenz-Beleg zum geänderten Parser): **4836/4836 Graphen Karp = Howard**, 4827 zusätzlich Brute-Force. Einzige inhaltliche Änderung gegenüber dem 007-Stand: `bhatiadeepak0805/OmniRoute_Project_Group_4` / `dag2_batch_pipeline_harsh` hat jetzt 1 Cross-Kante statt 0 und die `sensor_not_modeled`-Warnung auf `DAG_Codes/dag_2.py:480` ist weg (26 statt 27) — exakt der ADR-021-Fall, sonst nichts.

### Die vier Verifikations-Läufe (Artefakte in `scan/009_cli/`)

**Lauf 1a — Demo-Pipeline als Python-Aufruf** (`scan/009_cli/verif_demo.py`, Output `lauf1_demo.txt`): alle Prototyp-Pins getroffen, auch die drei What-if-Szenarien über die Report-Maschinerie:

```
Lambda           = 4.40 h  (Pin: 4.40)
Kreis kondensiert: [('monitor', 'monitor', 1)]
Kreis aufgeloest : ['core', 'features', 'retrain', 'score', 'monitor']
Drift bei T=3.0  = 1.40 h/Lauf  (Pin: 1.40)
What-if Cross-Kante monitor -> core entfernt (angefragt): Lambda = 2.5
What-if Task retrain = 0,8 s (angefragt): Lambda = 3.5999999999999996
What-if Task core = 0,55 s (angefragt): Lambda = 3.85

Monte Carlo 1000 Samples auf der Demo-Pipeline: 0.05 s
lambda_p50 = 4.51 h, lambda_p95 = 5.56 h
Anteil ueber T=3.0: 100%, konstant: ()
```

**Lauf 1b — CLI gegen ein minimales DAG-File mit `--assume-duration`** (voller Report in `scan/009_cli/lauf1_minidag_report.txt`, Fixture `minidag_mini.py`): Exit 0, Urteil stabil (λ = 600 s gegen T = 3600 s, Reserve 83,33 %), Kreis doppelt mit `depends_on_past, mini.py:6`, What-if angefragt `task=lade:120` → λ 120 s, 2 `dauer_angenommen`-Warnungen, Modellgrenzen-Fußzeile. Vollständig:

    eigenlag analyze
    ================

    DAG:        mini (mini.py:5, Schedule '@hourly')
    Takt T:     3600 s (60 min), Quelle: Schedule '@hourly'
    Dauern:     angenommen: 600 s je Task ohne Messung
    Statistik:  mean. Fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige Groesse; er ist ausreisserempfindlich, ein einzelner haengender Lauf kann ihn deutlich verschieben.
    Stichprobe: Laeufe je Task minimal 0, im Median 0.

    Urteil
    ------
    Stabil: Lambda = 600 s (10 min) liegt unter dem Takt T = 3600 s (60 min). Reserve: 83,33 %. Verspaetungen aus einem einzelnen Lauf klingen ab, statt sich aufzubauen.

    Kritischer Kreis
    ----------------
    Kondensiert (der Kreis in der Cross-Run-Matrix, sein Zyklusmittel ist Lambda):
      mini.lade -> mini.lade: Gewicht 600 s (10 min), 1 Periode zurueck [depends_on_past, mini.py:6]
    Aufgeloest ueber alle Segmente: mini.lade
    Der Weg zu einem kleineren Lambda fuehrt ueber diesen Kreis; eine Verkuerzung daneben aendert Lambda um exakt null. Ob eine einzelne Verkuerzung durchschlaegt oder ein zweiter Kreis mit gleichem Zyklusmittel uebernimmt, rechnet das What-if-Ranking unten nach.

    Monte Carlo
    -----------
    Lambda p50 = 600 s (10 min), Lambda p95 = 600 s (10 min) (1000 Stichproben, Lognormal-Fit aus p50/p95 je Task, Seed 20260715: derselbe Aufruf liefert dieselben Zahlen).
    p95 beantwortet, ob der Takt auch in einer schlechten Woche haelt, nicht nur im Durchschnitt.
    Anteil der Stichproben mit Lambda ueber dem Takt: 0 %.
    Konstant gesampelt (keine belastbare Streuung, angenommene oder duenne Dauern): mini.lade, mini.rechne. Die p95-Aussage unterschaetzt die Streuung dieser Tasks.

    What-if
    -------
    Basis: Lambda = 600 s (10 min). Sortiert nach neuem Lambda.
      1. Cross-Kante mini.lade -> mini.lade entfernt: kein Kreis mehr, Taktgrenze nicht anwendbar
      2. Task mini.lade = 120 s (angefragt): Lambda 120 s (2 min), Veraenderung -480 s
      3. Task mini.lade halbiert (auf 300 s): Lambda 300 s (5 min), Veraenderung -300 s
    Eine Optimierung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um exakt null. Das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten; alles andere ist fuer die Taktgrenze wirkungslos, so nuetzlich es fuer die Latenz eines Einzellaufs sein mag.

    Warnungen
    ---------
      - Dauer angenommen: mini.lade (keine Messung, 600.0 s)
      - Dauer angenommen: mini.rechne (keine Messung, 600.0 s)

    Modellgrenzen
    -------------
      - Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'.
      - Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig.
      - Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan.

**Lauf 2 — echtes Airflow 3.3.0:** `.venv-airflow` wiederverwendet, diesmal `AIRFLOW_HOME=data/airflow-home/` (bleibt liegen, unter dem gitignorten `data/`). `airflow db migrate`, die beiden 008-Testfall-DAGs nach `data/airflow-home/dags/`, je 6 Läufe `airflow dags test` (12/12 ok). Dann der Report, den ein echtes Team sehen würde — vollständig:

    eigenlag analyze
    ================
    
    DAG:        testfall_dop_sensor (testfall_dop_sensor.py:10, Schedule '@hourly')
    DAG:        testfall_gruppe (testfall_gruppe.py:9, Schedule '@hourly')
    Takt T:     3600 s (60 min), Quelle: Schedule '@hourly'
    Dauern:     Metadaten-DB sqlite:///data/airflow-home/airflow.db, Fenster 90 Tage
    Statistik:  mean. Fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige Groesse; er ist ausreisserempfindlich, ein einzelner haengender Lauf kann ihn deutlich verschieben.
    Stichprobe: Laeufe je Task minimal 6, im Median 6.
    
    Urteil
    ------
    Stabil: Lambda = 1,19 s liegt unter dem Takt T = 3600 s (60 min). Reserve: 99,97 %. Verspaetungen aus einem einzelnen Lauf klingen ab, statt sich aufzubauen.
    
    Kritischer Kreis
    ----------------
    Kondensiert (der Kreis in der Cross-Run-Matrix, sein Zyklusmittel ist Lambda):
      testfall_dop_sensor.arbeit -> testfall_dop_sensor.arbeit: Gewicht 1,19 s, 1 Periode zurueck [depends_on_past, testfall_dop_sensor.py:19]
    Aufgeloest ueber alle Segmente: testfall_dop_sensor.arbeit
    Der Weg zu einem kleineren Lambda fuehrt ueber diesen Kreis; eine Verkuerzung daneben aendert Lambda um exakt null. Ob eine einzelne Verkuerzung durchschlaegt oder ein zweiter Kreis mit gleichem Zyklusmittel uebernimmt, rechnet das What-if-Ranking unten nach.
    
    Monte Carlo
    -----------
    Lambda p50 = 1,18 s, Lambda p95 = 1,2 s (1000 Stichproben, Lognormal-Fit aus p50/p95 je Task, Seed 20260715: derselbe Aufruf liefert dieselben Zahlen).
    p95 beantwortet, ob der Takt auch in einer schlechten Woche haelt, nicht nur im Durchschnitt.
    Anteil der Stichproben mit Lambda ueber dem Takt: 0 %.
    
    What-if
    -------
    Basis: Lambda = 1,19 s. Sortiert nach neuem Lambda.
      1. Cross-Kante testfall_dop_sensor.arbeit -> testfall_dop_sensor.arbeit entfernt: kein Kreis mehr, Taktgrenze nicht anwendbar
      2. Task testfall_dop_sensor.arbeit halbiert (auf 0,59 s): Lambda 0,59 s, Veraenderung -0,59 s
    Eine Optimierung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um exakt null. Das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten; alles andere ist fuer die Taktgrenze wirkungslos, so nuetzlich es fuer die Latenz eines Einzellaufs sein mag.
    
    Warnungen
    ---------
    Keine.
    
    Modellgrenzen
    -------------
      - Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'.
      - Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig.
      - Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan.
    

λ = 1,19 s ist exakt `mean(arbeit)` aus der echten Metadaten-DB — deckungsgleich mit dem 008-Wert (1,186 s), jetzt durch die komplette CLI-Kette (parse → DB-Dauern → Report).

**Lauf 3 — Postgres-Wegwerf-Container** (offener Punkt aus 008, `scan/009_cli/verif_postgres.py`, Output `lauf3_postgres.txt`): `docker run --rm postgres:16`, dieselben Fixture-Zeilen wie die SQLite-Tests (inkl. failed/NULL/stale/fremder-DAG-Filterfälle) in beide Datenbanken, `from_metadata_db` gegen beide. **Der `percentile_cont`-Pfad liefert auf allen 4 Tasks exakt dieselben Werte wie die Python-Aggregation** (Toleranz 1e-9), inkl. des Interpolations-Falls `rare` (n=2, p95 = 3,9):

```
etl.extract     pg:  n=5 mean=30 p50=30 p95=48 op=PythonOperator
                lite:n=5 mean=30 p50=30 p95=48 op=PythonOperator
etl.grp.load    pg:  n=5 mean=3 p50=3 p95=4.8 op=PythonOperator
                lite:n=5 mean=3 p50=3 p95=4.8 op=PythonOperator
etl.rare        pg:  n=2 mean=3 p50=3 p95=3.9 op=None
                lite:n=2 mean=3 p50=3 p95=3.9 op=None
etl.wait        pg:  n=5 mean=60 p50=60 p95=60 op=ExternalTaskSensor
                lite:n=5 mean=60 p50=60 p95=60 op=ExternalTaskSensor

Postgres (percentile_cont) == SQLite (Python-Aggregation): identisch.
```

Container danach gestoppt und weg (`docker ps -a`: leer), nichts persistiert. Treiber `psycopg2-binary` nur in der Dev-venv, keine Projekt-Dependency.

**Lauf 4 — Flaggschiff `load_data_wikiviews`** mit `--assume-duration 300` (der Report für späteres Launch-Material) — vollständig:

    eigenlag analyze
    ================
    
    DAG:        load_data_wikiviews (dags/wikiviews/load_data.py:55, Schedule '@hourly')
    Takt T:     3600 s (60 min), Quelle: Schedule '@hourly'
    Dauern:     angenommen: 300 s je Task ohne Messung
    Statistik:  mean. Fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige Groesse; er ist ausreisserempfindlich, ein einzelner haengender Lauf kann ihn deutlich verschieben.
    Stichprobe: Laeufe je Task minimal 0, im Median 0.
    
    Urteil
    ------
    Stabil: Lambda = 600 s (10 min) liegt unter dem Takt T = 3600 s (60 min). Reserve: 83,33 %. Verspaetungen aus einem einzelnen Lauf klingen ab, statt sich aufzubauen.
    
    Kritischer Kreis
    ----------------
    Kondensiert (der Kreis in der Cross-Run-Matrix, sein Zyklusmittel ist Lambda):
      load_data_wikiviews.load_data -> load_data_wikiviews.load_data: Gewicht 600 s (10 min), 1 Periode zurueck [wait_for_downstream, dags/wikiviews/load_data.py:49]
        als Task-Pfad: load_data_wikiviews.сheck_data -> load_data_wikiviews.load_data
    Aufgeloest ueber alle Segmente: load_data_wikiviews.сheck_data -> load_data_wikiviews.load_data
    Der Weg zu einem kleineren Lambda fuehrt ueber diesen Kreis; eine Verkuerzung daneben aendert Lambda um exakt null. Ob eine einzelne Verkuerzung durchschlaegt oder ein zweiter Kreis mit gleichem Zyklusmittel uebernimmt, rechnet das What-if-Ranking unten nach.
    
    Monte Carlo
    -----------
    Lambda p50 = 600 s (10 min), Lambda p95 = 600 s (10 min) (1000 Stichproben, Lognormal-Fit aus p50/p95 je Task, Seed 20260715: derselbe Aufruf liefert dieselben Zahlen).
    p95 beantwortet, ob der Takt auch in einer schlechten Woche haelt, nicht nur im Durchschnitt.
    Anteil der Stichproben mit Lambda ueber dem Takt: 0 %.
    Konstant gesampelt (keine belastbare Streuung, angenommene oder duenne Dauern): load_data_wikiviews.create_success_file, load_data_wikiviews.load_data, load_data_wikiviews.load_to_postgres_trigger_clickhouse.clean_table, load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day, load_data_wikiviews.load_to_postgres_trigger_clickhouse.success, load_data_wikiviews.load_to_postgres_trigger_clickhouse.time_check, load_data_wikiviews.send_message_telegram, load_data_wikiviews.сheck_data. Die p95-Aussage unterschaetzt die Streuung dieser Tasks.
    
    What-if
    -------
    Basis: Lambda = 600 s (10 min). Sortiert nach neuem Lambda.
      1. Task load_data_wikiviews.сheck_data halbiert (auf 150 s): Lambda 600 s (10 min), Veraenderung +0 s
      2. Task load_data_wikiviews.load_data halbiert (auf 150 s): Lambda 600 s (10 min), Veraenderung +0 s
      3. Cross-Kante load_data_wikiviews.сheck_data -> load_data_wikiviews.сheck_data entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      4. Cross-Kante load_data_wikiviews.load_data -> load_data_wikiviews.сheck_data entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      5. Cross-Kante load_data_wikiviews.load_data -> load_data_wikiviews.load_data entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      6. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      7. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.success -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      8. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.time_check -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.time_check entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      9. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.time_check entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      10. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.clean_table -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.clean_table entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      11. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.success -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.clean_table entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      12. Cross-Kante load_data_wikiviews.load_to_postgres_trigger_clickhouse.success -> load_data_wikiviews.load_to_postgres_trigger_clickhouse.success entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      13. Cross-Kante load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      14. Cross-Kante load_data_wikiviews.send_message_telegram -> load_data_wikiviews.create_success_file entfernt: Lambda 600 s (10 min), Veraenderung +0 s
      15. Cross-Kante load_data_wikiviews.send_message_telegram -> load_data_wikiviews.send_message_telegram entfernt: Lambda 600 s (10 min), Veraenderung +0 s
    Eine Optimierung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um exakt null. Das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten; alles andere ist fuer die Taktgrenze wirkungslos, so nuetzlich es fuer die Latenz eines Einzellaufs sein mag.
    
    Warnungen
    ---------
      - Dauer angenommen: load_data_wikiviews.сheck_data (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.load_data (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.load_to_postgres_trigger_clickhouse.not_end_day (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.load_to_postgres_trigger_clickhouse.time_check (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.load_to_postgres_trigger_clickhouse.clean_table (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.load_to_postgres_trigger_clickhouse.success (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.create_success_file (keine Messung, 300.0 s)
      - Dauer angenommen: load_data_wikiviews.send_message_telegram (keine Messung, 300.0 s)
      - edge_dropped: dags/wikiviews/load_data.py:165 (Kanten-Ende nicht statisch aufloesbar)
      - edge_dropped: dags/wikiviews/load_data.py:166 (Kanten-Ende nicht statisch aufloesbar)
      - task_dag_inferred: dags/wikiviews/load_data.py:171 (create_success_file)
      - edge_dropped: dags/wikiviews/load_data.py:187 (Kanten-Ende nicht statisch aufloesbar)
    
    Modellgrenzen
    -------------
      - Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'.
      - Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig.
      - Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan.
    

λ = 600 s = die 007a-Struktur (2 × 300), Kreis kondensiert `load_data → load_data`, aufgelöst `сheck_data → load_data`, 8 `dauer_angenommen`-Warnungen — deckungsgleich mit 008.

**Zwei Befunde aus Lauf 4, beide behoben:**

1. **Alle What-if-Deltas sind +0 s, und das ist korrekt:** bei uniformen 300-s-Annahmen erreichen mehrere disjunkte Kreise dasselbe Zyklusmittel (600 s). Wer eine Kreis-Task halbiert, übergibt an den nächsten Kreis — λ bleibt. Der Report-Satz „Jede Verkürzung auf diesem Kreis senkt Lambda" widersprach damit den eigenen Zahlen im selben Report; er benennt jetzt den Gleichstand-Fall und verweist auf das Ranking. Ehrlichkeit vor Verkaufs-Satz.
2. **`ast.parse` schreibt für krumme Escapes in fremden Files (`"\;"` in einem bash_command) SyntaxWarnings auf stderr** und verschmutzt den CLI-Output. Fremde Files sind Systemgrenze: Warning jetzt unterdrückt, per Test gepinnt (zuerst rot).

**pipx-Rauchtest:** pipx via apt installiert (1.8.0), `pipx install .` → „installed package eigenlag 0.1.0, installed using Python 3.14.4", `eigenlag --help` und `eigenlag analyze <minidag> --assume-duration 600 --json` liefern über den Entry-Point dieselben Ergebnisse. Die Mechanik trägt; README/Versionierung bleiben 011.

**Verifiziert:**

```
pytest: 312 passed (38 neue: 4 Parser/ADR-021, 6 Monte Carlo, 14 Report, 13 CLI, 1 SyntaxWarning)
ruff check: All checks passed!  |  ruff format --check: 44 files already formatted (ein Zwischenstand zaehlte 64, weil pipx-Build-Artefakte build/ und eigenlag.egg-info/ noch nicht gitignored waren)
mypy: Success: no issues found in 20 source files (eigenlag/)
```

Kern weiterhin ohne Pflicht-Dependencies (`dependencies = []` unverändert, Akzeptanz „null oder numpy mit Messbeleg": null, Messbeleg 0,05 s steht oben).

**Modell-Notiz für den Orchestrator:** Ein Lauf mit `--samples 1000` auf der Demo zeigt `lambda_p50 = 4.51` — der MC-Median liegt **über** dem Punkt-λ auf mean (4.40), weil der Lognormal-Fit aus p50/p95 einen anderen Mittelwert impliziert als das arithmetische mean der Messung und das Maximum konkurrierender Pfade nach oben zieht. Das ist konsistent (zwei verschiedene Schätzer, beide ausgewiesen), sollte aber im 010-Gate nicht als „Punkt-λ" gegen Schwellen laufen: das Gate soll explizit festlegen, welcher der beiden Werte gilt.

---

## 009a — Abnahme CLI durch den Orchestrator (2026-07-15)

**Abgenommen.** Gates unabhängig nachgefahren (312 Tests, ruff, mypy über 44 Files). Vor allem aber: **das CLI als Nutzer benutzt, nicht als Log-Leser** — `eigenlag analyze` über den pipx-Entry-Point auf dem Flaggschiff laufen lassen und den vollen Report gelesen. Er hält, was die Spec verlangt: Urteil zuerst (mit Reserve in Prozent), Kreis doppelt mit Datei:Zeile, Monte Carlo mit offengelegter Konstant-Sampling-Liste, nie abschaltbarer Warnblock. Die Formulierungen sind ruhig und tragen ihre Grundlage bei sich.

**Die MC-Abweichung der Session reproduziert und für 010 entschieden.** Eigener Lauf auf der Demo-Pipeline mit eigener Streuungs-Annahme: Punkt-λ = 4.40, MC-λ_p50 = 4.45 (Session: 4.51 mit anderen Fit-Parametern; Richtung identisch). Der Effekt ist systematisch und erwartbar: λ ist ein Maximum über Kreis-Summen und damit konvex in den Dauern; dazu liegt der Erwartungswert einer Lognormal über ihrem Median. MC-λ_p50 wird Punkt-λ also immer übersteigen. **Entscheidung für die 010-Spec: Das Gate vergleicht Punkt-λ gegen Punkt-λ** (dieselbe Statistik vor und nach dem Diff — deterministisch, bit-stabil, jede Differenz einer Code-Änderung zuordenbar; der systematische Bias kürzt sich beim Gleiches-mit-Gleichem-Vergleich heraus). Monte Carlo bleibt Risiko-Sicht im Report, läuft aber nicht gegen Schwellen.

**Ein Befund aus dem echten Gebrauch, den das Session-Log nicht nennt:** Bei uniformen Assume-Dauern ist das What-if-Ranking eine Flut — 15 Zeilen, alle "+0 s", inklusive aller Cross-Kanten, die gar nicht auf dem kritischen Kreis liegen. Der Schlusssatz behauptet zudem "das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten", tatsächlich zeigt es **alle** Cross-Kanten. Inhaltlich ist +0 korrekt (Gleichstand mehrerer Kreise, von der Session sauber im Kreis-Absatz erklärt), aber die Darstellung liest sich als Rauschen — und der Feedback-Meilenstein hängt an genau diesem Report. **Für 010 (oder als Mini-Fix davor): Null-Delta-Zeilen zu einer Sammelzeile kompaktieren** ("13 weitere Szenarien ändern λ nicht") **und den Schlusssatz an das tatsächliche Verhalten angleichen.** Kein Blocker, aber vor dem ersten fremden Leser zu erledigen.

**Sonst bestätigt:** Postgres-Punkt aus 008 geschlossen (percentile_cont = Python auf denselben Fixtures), ADR-021 umgesetzt mit Neu-Lauf des Graph-Checks (4836/4836, einzige Änderung die erwartete OmniRoute-Kante), Demo-Pins durch die neue Maschinerie, MC in stdlib bei 0,05 s je 1000 Samples — Faktor 100 unter dem Messvorbehalt, `dependencies = []` hält seit Session 004.

**Damit ist der Feedback-Meilenstein erreicht.** Das Tool ist installierbar, läuft gegen echte Metadaten-DBs und erklärt sich selbst. Die nächste Entscheidung ist keine technische: an welche 2–3 Teams geht es zuerst. Kriterien für ein gutes Erst-Team: sub-täglicher Takt im Einsatz, `depends_on_past`/`wait_for_downstream` oder Sensor-Ketten in den DAGs, Zugriff auf die eigene Metadaten-DB, und jemand, der einen deutschen Report liest (sonst zieht i18n vor den Feedback-Termin). Davids Entscheidung.

---

## Session 010 — CI-Gate `eigenlag check --against REF` (2026-07-15)

**Was gemacht wurde.** Zuerst die beiden Report-Korrekturen aus der Abnahme 009a (eigener Commit), dann das Gate: `eigenlag/gate.py` (Worktree-Mechanik, Kanten-Vergleich je DAG, compose_check/render_check aus einer Quelle wie in 009), CLI-Befehl `check` in `cli.py`, 26 neue Gate-Tests plus 8 neue Report-Tests (alle zuerst rot gesehen: Gate-Tests fielen mit Collection-Error, solange `gate.py` fehlte; Report-Tests fielen auf dem alten Renderer), ADR-022, `docs/ci-gate.md` mit GitHub-Actions-Beispiel (nicht ausgefuehrt — kein Netz, kein Posten).

### Report-Korrekturen 009a, am Flaggschiff sichtbar

Derselbe Aufruf wie 009a Lauf 4 (`eigenlag analyze . --dag-id load_data_wikiviews --assume-duration 300` im Clone `Gleb01548__russian_wiki_view_db`), Kopf byte-identisch zur 009a-Fassung, What-if vorher 15 Rauschzeilen, nachher:

    What-if
    -------
    Basis: Lambda = 600 s (10 min). Sortiert nach neuem Lambda.
      15 weitere Szenarien aendern Lambda nicht: 3 Kreis-Gleichstaende, 12 Kanten ausserhalb des kritischen Kreises.
    Eine Optimierung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um exakt null. Das Ranking rechnet deshalb die Kreis-Tasks und alle Cross-Kanten durch; was Lambda nicht aendert, ist fuer die Taktgrenze wirkungslos, so nuetzlich es fuer die Latenz eines Einzellaufs sein mag.

Voller Report in `scan/010_gate/lauf0_wikiviews_report_v2.txt`, direkt neben der 009a-Fassung (`scan/009_cli/lauf4_wikiviews_report.txt`). Die 3 Kreis-Gleichstaende sind von Hand nachvollziehbar: die zwei Halbierungen der Kreis-Tasks (`сheck_data`, `load_data`) plus die Kreis-Kante `load_data -> сheck_data`; die uebrigen 12 Cross-Kanten liegen nicht auf dem kritischen Kreis.

**Implementer-Entscheidungen dabei (je eine Zeile Begruendung):**

- **Schlusssatz an das Verhalten angepasst, nicht umgekehrt:** die `--json`-Vollstaendigkeit aller Szenarien ist die Schnittstelle, auf der das Gate aufsetzt, und das Rausch-Problem im Text loest die Sammelzeile — das Verhalten zu beschneiden haette Information gekostet, der Satz war schlicht falsch.
- **Angefragte Szenarien (`--what-if`) werden nie kompaktiert:** wer explizit fragt, bekommt die Zeile, auch bei +0.

### Das Gate: Verifikation end-to-end ueber die pipx-Installation

`pipx install --force .` (eigenlag 0.1.0, Python 3.14.4), dann alle Laeufe ueber den Entry-Point. Fixture-Repo mit echter Git-Historie (drei Commits, Tags v1/v2/v3; v1 ohne Cross-Run-Kante, v2 mit `wait_for_downstream` in `default_args` wie im Flaggschiff, v3 wieder ohne), gebaut im Scratchpad aus den Test-Fixtures.

**Lauf 1 — v2 gegen v1 (Struktur-Modus, Default-Regel): Exit 3.** Kommentar vollstaendig:

    **eigenlag check: ausgeloest** — load_data_wikiviews: neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse bei sub-taeglichem Takt (T = 3600 s (60 min)).

    Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder --assume-duration.

    ### load_data_wikiviews

    - Lambda: kein Kreis -> 2 Task-Einheiten (vorher -> nachher)
    - Takt T: 3600 s (60 min), Quelle: Schedule '@hourly'
    - Neue Cross-Run-Kanten (5):
      - `load_data_wikiviews.check_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.load_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.load_data -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
    - **Ausgeloest:** neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse bei sub-taeglichem Takt (T = 3600 s (60 min))
    - **Ausloesende Kante:** `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10)
    - Kritischer Kreis, kondensiert: `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file`, Gewicht 2 Task-Einheiten, 1 Periode zurueck [wait_for_downstream, pipeline.py:10]
    - Aufgeloest: load_data_wikiviews.load_data -> load_data_wikiviews.create_success_file
    - Behebung: Die ausloesende Kante zu entfernen behebt den Fail. Eine Zeit-Aussage (Lambda gegen T in Sekunden) braucht eine Dauern-Quelle: --db oder --assume-duration.

    ---
    _Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

**Lauf 2 — v3 gegen v2 (Kante wieder entfernt): Exit 0.** Kommentar vollstaendig:

    **eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt (`/tmp/claude-1000/-mnt-data-projects-eigenlag/43709c11-0215-4778-b8c2-393860dac021/scratchpad/fixture-repo/repo/dags` gegen `v2`).

    Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder --assume-duration.

    ### load_data_wikiviews

    - Lambda: 2 Task-Einheiten -> kein Kreis (vorher -> nachher)
    - Takt T: 3600 s (60 min), Quelle: Schedule '@hourly'
    - Entfallene Cross-Run-Kanten: `load_data_wikiviews.check_data -> load_data_wikiviews.check_data`, `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file`, `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data`, `load_data_wikiviews.load_data -> load_data_wikiviews.check_data`, `load_data_wikiviews.load_data -> load_data_wikiviews.load_data`

    ---
    _Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

**Lauf 3 — unveraenderter Stand (v3 gegen v3): Exit 0.** Kommentar vollstaendig:

    **eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt (`/tmp/claude-1000/-mnt-data-projects-eigenlag/43709c11-0215-4778-b8c2-393860dac021/scratchpad/fixture-repo/repo/dags` gegen `v3`).

    Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder --assume-duration.

    1 DAG(s) ohne Aenderung an Cross-Run-Kanten oder Lambda.

    ---
    _Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

**Lauf 4 — Sekunden-Modus (`--assume-duration 2500`): Exit 3, Auftrags-Regel woertlich.** Lambda = 5000 s ueber T = 3600 s, Behebungs-Hinweis nennt ehrlich, dass keine einzelne Standard-Aenderung reicht (mehrere Kreise mit gleichem Zyklusmittel bei uniformen Dauern — derselbe Gleichstand-Befund wie in 009a). Kommentar vollstaendig:

    **eigenlag check: ausgeloest** — load_data_wikiviews: neue Cross-Run-Kante und Lambda = 5000 s (83,33 min) ueber dem Takt T = 3600 s (60 min).

    ### load_data_wikiviews

    - Lambda: kein Kreis -> 5000 s (83,33 min) (vorher -> nachher)
    - Takt T: 3600 s (60 min), Quelle: Schedule '@hourly'
    - Neue Cross-Run-Kanten (5):
      - `load_data_wikiviews.check_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.load_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.load_data -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
      - `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
    - **Ausgeloest:** neue Cross-Run-Kante und Lambda = 5000 s (83,33 min) ueber dem Takt T = 3600 s (60 min)
    - **Ausloesende Kante:** `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10)
    - Kritischer Kreis, kondensiert: `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file`, Gewicht 5000 s (83,33 min), 1 Periode zurueck [wait_for_downstream, pipeline.py:10]
    - Aufgeloest: load_data_wikiviews.load_data -> load_data_wikiviews.create_success_file
    - Behebung: Keine einzelne Standard-Aenderung (Kreis-Task halbiert, Cross-Kante entfernt) bringt Lambda unter T; der Kreis traegt an mehreren Stellen dasselbe Zyklusmittel.

    ---
    _Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

**Lauf 5 — Selbst-Anwendung als Negativ-Probe: Exit 0 mit Hinweis, kein Absturz.** `eigenlag check eigenlag --against HEAD` in diesem Repo (das Package-Verzeichnis enthaelt keine DAGs; das Repo-Root waere keine Negativ-Probe, `scanner/fixtures/` und `data/repos/` enthalten absichtlich DAG-Files):

    **eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt (`eigenlag` gegen `HEAD`).

    Keine DAGs in beiden Staenden — nichts zu pruefen.

    ---
    _Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

`git status --porcelain` und `git worktree list` vor und nach den Laeufen identisch — das Nutzer-Repo bleibt unangetastet; derselbe Beleg ist als Test gepinnt, inklusive Exception-Fall (Worktree verschwindet auch, wenn im with-Block geworfen wird).

### Implementer-Entscheidungen am Gate (je eine Zeile Begruendung)

- **Struktur-Modus-Default-Regel** (Detail zu ADR-022, dort dokumentiert): „Lambda_nachher > T" ist in Task-Einheiten gegen Sekunden nicht auswertbar — woertlich implementiert waere das Default-Gate in der CI wirkungslos; stattdessen loest eine neue Kante aus, die einen Kreis schliesst, bei sub-taeglichem Takt (deckungsgleich ADR-018), und der Kommentar sagt, dass die Zeit-Aussage eine Dauern-Quelle braucht.
- **Behebungs-Hinweis:** unter den Standard-Szenarien, die Lambda unter T bringen, gewinnt das mit dem groessten neuen Lambda — die am wenigsten invasive Aenderung, die reicht; Kreis-Aufloesungen nur, wenn kein endliches Szenario existiert; existiert gar keines, sagt das Gate das (Spec: „wenn keine existiert, das sagen").
- **Kanten-Identitaet ohne Datei:Zeile** (`src, dst, periods, signal`, namespaced): eine verschobene Zeile ist keine neue Kante — sonst wuerde jedes Umformatieren das Gate ausloesen.
- **DAGs ohne statische `dag_id` sind nicht vergleichbar** und werden als Hinweis benannt (mit Datei:Zeile), kein Fail: raten waere schlimmer als benennen.
- **`--dag-id` unbekannt ist Bedienfehler (Exit 1), nicht Exit 2:** die 2 bleibt bei `analyze` reserviert, die Raeume ueberlappen nicht (Spec-Vorentscheidung 5).
- **`--comment-file` schreibt Markdown zusaetzlich zur stdout-Ausgabe:** so traegt `--json` (stdout maschinenlesbar) plus `--comment-file` (Markdown fuers Posten) in einem Aufruf — der CI-Job braucht beide.
- **Modul heisst `gate.py`** (CLI-Befehl bleibt `check` wie in der Spec): `wiki/architecture.md` fuehrte das Modul seit 009 unter diesem Namen, Wiki ist Wahrheit.

### Gates

```
pytest: 344 passed (34 neue: 26 Gate, 8 Report-Kompaktierung; jedes Modul zuerst rot)
ruff check: All checks passed!  |  ruff format --check: 46 files already formatted
mypy: Success: no issues found in 22 source files (eigenlag/)
pipx install --force . -> eigenlag 0.1.0; alle 5 Gate-Laeufe oben ueber den Entry-Point
```

Pflicht-Dependencies unveraendert null (`dependencies = []`); das Gate braucht nur `subprocess` + git, das ohnehin da ist, wo ein `--against REF` Sinn ergibt.

**Was ueberrascht hat:** Die Auftrags-Regel „neue Kante und Lambda > T" ist im Struktur-Modus, dem realistischen CI-Default, gar nicht auswertbar — die Spec-Vorentscheidungen 3 und 4 standen an dieser Stelle in Spannung (Struktur-Modus als Default, aber die Fail-Bedingung braucht Sekunden). Die Aufloesung steht in ADR-022; falls der Orchestrator die Kreis-plus-sub-taeglich-Regel anders schneiden will, ist sie eine einzelne Funktion (`gate._dag_row`, Verdict-Block) mit gepinnten Tests.

---

## 010a — Abnahme CI-Gate durch den Orchestrator (2026-07-15)

**Abgenommen.** Session 010 lief erstmals als vom Orchestrator gestartete Hintergrund-Implementer-Session (Davids Anweisung, er war abwesend). Abnahme wie immer unabhängig:

- Gates nachgefahren: 344 Tests, ruff (46 Files), mypy grün.
- **Das Gate selbst bedient, an einem eigenen Fixture-Repo**, das nicht aus der Session stammt (frisches `git init`, v1 ohne Kante, v2 mit `depends_on_past=True` bei `*/30 * * * *`): v2 gegen v1 → Exit 3, Kommentar nennt die auslösende Kante mit `etl.py:7` und Signal-Art, Kreis kondensiert und aufgelöst, Behebungs-Hinweis; unverändert → Exit 0; Nutzer-Repo unangetastet, kein Rest-Worktree.
- Flaggschiff-Report-Korrektur bestätigt: 15 Null-Delta-Zeilen → eine Sammelzeile ("3 Kreis-Gleichstände, 12 Kanten außerhalb"), Schlusssatz beschreibt jetzt das Verhalten.

**Die zentrale Implementer-Entscheidung wird bestätigt — und der Fehler lag in meiner Spec.** Vorentscheidung 3 behauptete, der Struktur-Modus decke "genau den Fall, den der Auftrag nennt" ab, und Vorentscheidung 4 verlangte gleichzeitig die wörtliche Regel "neue Kante **und** λ > T". Beides zusammen geht nicht: Im Struktur-Modus ist λ in Task-Einheiten und T in Sekunden, der Vergleich ist unauswertbar — das Default-Gate wäre in einer CI ohne Metadaten-DB wirkungslos gewesen, also im häufigsten Einsatzfall. Die Auflösung der Session (Struktur-Modus: neue Kante, die einen Kreis schließt, bei sub-täglichem Takt → deckungsgleich mit der Risiko-Definition aus ADR-018; mit Dauern-Quelle: Auftrag wörtlich) ist genau richtig, sauber in ADR-022 dokumentiert und an einer einzigen Stelle implementiert. Mein Kommentar-Fixture-Lauf oben ist der Beleg, dass sie praktisch funktioniert.

**Damit ist 010 komplett. Es fehlt nur noch 011** (Packaging, englisches README, Report-Sprachfassung), dann ist der Feedback-Meilenstein erreichbar. Die 011-Spec schreibt der Orchestrator als Nächstes; die Session dazu startet David selbst.

### Session 011 — Packaging, englisches README, Report-Sprachfassung (2026-07-15)

**Das Repo ist veröffentlichungsreif.** Englisch ist Report-Default, Deutsch bleibt vollwertig unter `--lang de` (ADR-023). Lizenz MIT (Davids Entscheidung beim Session-Start).

- **`eigenlag/messages.py`**: zwei Nachrichten-Kataloge (EN/DE) als schlichte dicts, keine i18n-Bibliothek. `fmt`/`dur` (Dezimaltrenner sprachabhängig, Einheiten gleich), `perioden`, `scenario_label`, `t`. `messages_test.py` erzwingt Katalog-Vollständigkeit (jeder Key in beiden Sprachen, kein stiller Fallback).
- **`report.render(d, lang)`** und **`gate.render_check(d, lang)`** ziehen jede Formulierung aus dem Katalog. `compose()`/`compose_check()` bleiben sprachneutral; die deutschen Prosa-Felder im `--json` bleiben eingefroren, dazu kamen sprachneutrale Struktur-Felder (`art`/`task`/`src`/`dst` je What-if-Zeile; `gruende_codes`/`behebung_code`/`hinweis_codes` im Gate), aus denen der Renderer den Text pro Sprache baut. Der bewusste Schnitt an „compose nicht anfassen" steht in ADR-023.
- **CLI (Spec-Punkt 4)**: argparse-Hilfe, Fehlermeldungen und Quellen-Beschriftungen einsprachig englisch; `--lang en|de` an beiden Subcommands, Default `en`. Terse Diagnose-Details (durations/parse_airflow/analyze) auf Englisch gezogen — geflaggte Grenze, siehe ADR-023.
- **README.md** neu, englisch: Sauerteig-Intro, Quickstart mit **echtem** instabilem Lauf (λ = 4000 s > T = 3600 s, Drift 400 s/Lauf, Mehr-Task-Kreis über `wait_for_downstream`), CI-Gate mit GitHub-Actions-Beispiel, „What it will tell you" mit dem Wikimedia-Sweep (30 über Takt, 29 driften nicht), Pflicht-Limitations-Abschnitt. `docs/ci-gate.md` auf Englisch.
- **Packaging**: `pyproject.toml` mit englischer `description`, `license = "MIT"` (SPDX/PEP 639), `license-files`, `classifiers`, `keywords`, `urls`; `LICENSE`-File. `dist/` in `.gitignore`.

**Verifiziert (Belege):**
- `pytest`: **356 passed** (messages_test, i18n_test neu; DE-Tests auf `--lang de`/`render(…, "de")` umgestellt, EN-Tests ergänzt). `ruff check`/`ruff format --check` (25 Files), `mypy eigenlag/` (25 Files) grün.
- `python -m build`: sdist + wheel gebaut, LICENSE eingebettet (`dist-info/licenses/`), METADATA trägt `License-Expression: MIT`. Wheel in frische venv installiert, `eigenlag --help` daraus englisch.
- `pipx install --force .`, dann über den Entry-Point: `analyze` EN + DE, `check` EN + DE (beide Exit 3), `--json` **byte-identisch** EN vs. DE bei `analyze` und `check` (per `diff -q` belegt).

**Was überrascht hat:** Die Spec-Klausel „compose() wird nicht angefasst" und „der ganze Report zweisprachig" widersprachen sich für die in `compose()` generierten Prosa-Felder (What-if-Labels, Gate-Gründe). Ein korrekter englischer Report braucht die Generierungs-Eingaben zur Render-Zeit — additive, sprachneutrale Struktur-Felder waren der einzige Weg, der `--json` byte-identisch hält. In ADR-023 als bewusster Schnitt dokumentiert und für den Orchestrator geflaggt.

---

## 011a — Abnahme Packaging/Sprachfassung durch den Orchestrator (2026-07-15)

**Abgenommen und gepusht** (der Commit lag lokal vor, die Session hatte vor dem Push rückgefragt). Unabhängig geprüft: 356 Tests, ruff, mypy grün; `pipx install --force .` und beide Sprachfassungen selbst gefahren (EN-Default-Urteil, DE unter `--lang de`), `--json` per `diff` byte-identisch über beide Sprachen, README als Erstleser gelesen.

**Beide geflaggten Entscheidungen bestätigt:** (1) Der `compose()`-Schnitt (bestehende Keys und deutsche Werte unverändert, additive sprachneutrale Struktur-Felder für den Renderer) ist die richtige Auflösung des Spec-Widerspruchs — meine Spec verlangte gleichzeitig "compose() nicht anfassen" und einen korrekten englischen Report, was nicht beides ging, weil deutsche Prosa in generierten Feldern steckte. Der Byte-Identitäts-Test belegt, dass die Gate-Schnittstelle stabil blieb. (2) Einsprachig englische Diagnose-Details im DE-Report sind als kleines Folge-Ticket notiert, kein Blocker.

**Damit ist die Roadmap bis 011 komplett.** Das Repo ist veröffentlichungsreif, veröffentlicht ist nichts. Vor dem Schalter kommen noch: Session 012 (Beschleunigungsplan — die Produkt-Ebene aus positioning.md), Launch-Kit (Demo-Einstieg, CI-Workflow als Vertrauenssignal, Launch-Texte), dann Davids Go.

---

## Session 012 — Beschleunigungsplan: aus der Diagnose wird das Produkt (2026-07-16)

**Aus „hier ist deine Grenze" wird „hier ist die Änderung, die den Unterschied kauft".** Der Report handelt jetzt: der Beschleunigungsplan ersetzt die What-if-Sektion und formuliert jeden Befund als unbeanspruchte Reserve statt als Mangel. Grundlage ADR-024.

- **`eigenlag/plan.py::build_plan`** (neu): reine, sprachneutrale Funktion. Reichert die What-if-Zeilen an — Kanten-Art (A `depends_on_past`, B `wait_for_downstream`, C `external_task_sensor`, D `include_prior_dates`, G `max_active_runs`, dbt-E `is_incremental`), Katalog-Schlüssel, λ_neu, Delta absolut und Prozent, `macht_tragfaehig`, verdict-abhängige `gewinn`-Felder. Instabil: Paar-Rechnung der drei wirksamsten Aktionen (`itertools.combinations`, drei Paare, keine Explosion). Signal-Herkunft aus den geparsten DAGs; ein direkt gebautes Pipeline-Objekt (die Demo) trägt kein Signal und bekommt keinen Katalog-Text.
- **`eigenlag/messages.py`**: Behebungs-Katalog `plan_fix_*` je Kanten-Art in EN und DE, plus `plan_fix_task_halved`; die Plan-Render-Keys (`plan_header`, `plan_basis`, `plan_gewinn_*`, `plan_headroom_*`, `plan_paar_*`, `plan_schluss`). Die sieben reinen What-if-Render-Keys entfielen, die Szenario-Label- und Sammelzeilen-Keys blieben (von Plan und Gate geteilt). Der bestehende `messages_test`-Parity-Test erzwingt EN/DE-Vollständigkeit automatisch; ein neuer Test in `plan_test.py` erzwingt zusätzlich, dass jede Kanten-Art einen Katalog-Eintrag hat.
- **`eigenlag/report.py`**: `_what_if_text` ersetzt durch `_plan_text` (liest `d["plan"]`), Render-Reihenfolge Urteil → Kreis → **Plan** → Monte Carlo → Warnungen. `compose()` rechnet die What-if-Zeilen einmal, legt sie unter `what_if` (eingefroren, Gate liest sie) und baut daraus additiv `plan`.
- **Zwei Gewinn-Formen, exakt (ADR-024, Punkt 2):** instabil „makes your current schedule sustainable" ⇔ λ_neu < T, plus weggeräumte Drift (λ − T); stabil Headroom (86400/λ − 86400/T Läufe/Tag mehr, **bis zu** T − λ frischer) mit Fußzeile „Untergrenze ohne Betriebsreserve", keine Schedule-Empfehlung.

**Verifiziert (Belege in `scan/012_plan/`):**

```
$ pytest -q            # im .venv (mit sqlalchemy)
370 passed
$ ruff check eigenlag/         → All checks passed!
$ ruff format --check eigenlag/ → 27 files already formatted
$ mypy eigenlag/               → Success: no issues found in 27 source files
```

- **Demo** (`lauf1_demo_plan_en.txt`), Prototyp-Ground-Truth λ = 4.40 h, T = 3.0 h, instabil, Dauern ×3600 skaliert damit `dur()` Stunden ausgibt: der Plan markiert die Quality-Gate-Kante `monitor → core` entfernen (λ_neu = 2.50 h) als „makes your current schedule sustainable" (−43,18 %, räumt 84 min/Lauf Drift weg), `retrain` halbiert (→ 3.60 h) und `core` halbiert (→ 3.85 h) senken λ, retten den Takt aber nicht. Genau die Verkaufsgeschichte: das GPU-Upgrade rettet den Takt nicht, die kostenlose Architektur-Änderung schon.
- **Flaggschiff** `load_data_wikiviews` (`--assume 300`, stabil, λ = 600 s, T = 3600 s), EN + DE (`lauf2_wikiviews_en.txt`/`_de.txt`): Headroom 120 Läufe/Tag mehr (144 − 24), bis zu 3000 s (50 min) frischer — von Hand nachgerechnet, exakt getroffen.
- **Synthetischer Zwei-Loop-Fall** (`lauf3_pair_en.txt`): zwei gleich schwere Selbst-Loops (je 5 h) über T = 4 h. Keine Einzel-Aktion rettet T; die Paar-Rechnung zeigt „beide Selbst-Kanten zusammen entfernt → kein Kreis mehr".
- `pipx install --force .`, Läufe über den Entry-Point `eigenlag`; `--json` EN vs. DE per `diff -q` byte-identisch, `plan`-Key additiv präsent.
- README-Quickstart auf einen echten, aktualisierten Lauf umgestellt (`lauf4_readme_feature_pipeline.txt`, Fixture `scan/012_plan/readme_demo/`): dasselbe instabile `feature_pipeline` wie in 011, jetzt mit dem Plan-Abschnitt statt der What-if-Liste.

**Was überrascht hat:** Der Header-Rename „What-if" → „Beschleunigungsplan" brach vier Report-/i18n-Tests, die den gerenderten What-if-Text prüften — drei davon injizierten früher `d["what_if"]` und rendern; da `render()` jetzt `d["plan"]` liest, mussten sie auf die Plan-Struktur (`plan_mit`/`paktion`) umgestellt werden. Spec-getriebene Änderung, keine an die Ausgabe angepasste Erwartung: die JSON-`what_if`-Struktur-Tests blieben unverändert grün, der `plan`-Key ist rein additiv. Zweite Überraschung: „keine Einzel-Aktion rettet T" tritt sauber nur bei ko-bindenden Mehrfach-Kreisen auf — bei einem einzelnen Kreis löst das Entfernen der bindenden Kante ihn immer ganz auf und ist damit schon eine rettende Einzel-Aktion. Die Paar-Fixture musste darum zwei gleich schwere, disjunkte Loops nehmen.

---

## 012a — Abnahme Beschleunigungsplan durch den Orchestrator (2026-07-16)

**Abgenommen.** Gates nachgefahren (370 Tests, ruff, mypy), Flaggschiff selbst über den Entry-Point gefahren und die Gewinn-Rechnung von Hand geprüft: 24 Läufe/Tag bei T = 3600 s gegen 144 bei λ = 600 s ergibt exakt die 120 aus dem Report, Frische-Delta 3000 s = 50 min, "up to" und die Keine-Marge-Fußzeile stehen da. Demo-Pins nachgerechnet: −43,18 % = 6840/15840, Drift 5040 s = 84 min/Lauf; nur die Kanten-Entfernung trägt die Tragfähigkeits-Markierung, die beiden Halbierungen sagen ehrlich "does not make T sustainable on its own".

**Beide geflaggten Schnitte bestätigt.** Der Header-Rename mit Test-Umstellung ist spec-getrieben (die eingefrorene `what_if`-JSON-Struktur blieb unangetastet, `plan` ist additiv). Und "Katalog-Text nur bei geparster Signal-Herkunft" ist genau das richtige Ehrlichkeits-Prinzip: Die Demo trägt kein geparstes Signal, also bekommt sie kein Muster-Wissen angehängt, das wir nicht haben.

**Der Demo-Report ist jetzt das Marketing-Artefakt, das er sein sollte.** Ein Screenshot dieser Plan-Sektion erzählt die ganze Produktgeschichte: die kostenlose Architektur-Änderung rettet den Takt, das GPU-Upgrade nicht.

**Ein Formulierungs-Detail für 013 (kein Blocker):** Der generische Halbierungs-Katalogtext endet auf "not detail about the foreign task" — "foreign task" ist ein Deutschismus (fremde Task) und liest sich im Englischen schief. Da die Demo-Plan-Sektion künftig das meistgesehene Artefakt ist, gehört der Satz in 013 poliert (etwa "the plan shows the arithmetic; how to split *your* task is yours to judge").

---

## Session 013 — Launch-Kit: der 60-Sekunden-Einstieg und die Texte (2026-07-16)

**Alles für Davids Schalter liegt bereit; veröffentlicht hat diese Session nichts.** Kein Upload, kein Sichtbarkeits-Wechsel, kein Post — nur Material, wie die Spec es verlangt.

- **`eigenlag demo`** (neu, tests-zuerst: `demo_test.py` rot gesehen, dann grün): eingebauter Subcommand, rendert den vollen Report der Prototyp-Pipeline (λ = 15840 s = 4.4 h bei T = 3 h) über denselben `compose()`/`render()`-Pfad wie `analyze`, EN und DE. Kopfzeile deklariert das eingebaute Beispiel, Fußzeile zeigt `eigenlag analyze your/dags --assume-duration 300`. Kein Netz, keine Dateien; über den pipx-Entry-Point gemessen: **0,113 s**. Die MC-Streuung (p95 = 1,5 × p50) und n = 40 sind Beispiel-Annahmen wie im abgenommenen 012-Artefakt und stehen deklariert in der Dauern-Quelle.
- **Fixture-Umzug (Single Source):** DUR/INTRA/CROSS + `demo()` von `maxplus_test.py` nach `eigenlag/demo.py`, weil der Subcommand sie ausliefert; `maxplus_test`/`plan_test` importieren von dort, alle Pins blieben unverändert in den Tests.
- **012a-Feinschliff:** `plan_fix_task_halved` neu formuliert — EN "…the plan shows the arithmetic, whether and how to split is yours to judge.", DE "…ob und wie geteilt wird, laesst sich nur am konkreten Task entscheiden." README-Sweep (2 Stellen im Quickstart-Block). Die `demo_fuss`-Erstfassung trug einen Gedankenstrich als Satztrenner (Schreibregel-Verstoß in sichtbarem Output) und wurde vor dem finalen GIF umformuliert.
- **GIF:** `launch/demo.tape` (vhs 0.11.0 + ttyd 1.7.7, beide nach `~/.local/bin`; Render braucht `VHS_NO_SANDBOX=true`, Chromium-Sandbox auf dem Server nicht nutzbar) → `assets/demo.gif`: **355 978 Bytes (< 3 MB), 16,64 s (< 30 s)**. pipx-Zeile gestellt (Shell-Funktion, im Tape kommentiert), `eigenlag demo` läuft echt. Endframe per ffmpeg extrahiert und gesichtet: hält auf der Plan-Sektion, Aktion 1 mit −43,18 % und Tragfähigkeits-Satz sichtbar. Im README ganz oben eingebettet (Pfad `assets/demo.gif` existiert relativ zum README, mehr Markdown-Check braucht die Einbettung nicht).
- **CI:** `.github/workflows/ci.yml`, Matrix 3.12/3.14, Schritte exakt wie die Frisch-Clone-Probe: `pip install -e ".[db,scanner]" pytest ruff mypy` → `pytest -q` → `ruff check .` → `ruff format --check .` → `mypy`. Ressourcen-Prüfung der Suite: **kein Test braucht Netz, Docker oder `data/`** (fetch gemockt, clone_test/gate_test mit lokalen Repos und inline git-Identität, `data/` in norecursedirs) — Skip-Marker waren darum nicht nötig; was fehlt, sind nur die Extras db (sqlalchemy, durations-Tests) und scanner (pyyaml, dbt-Analyse). Deshalb installiert die CI `.[db,scanner]` statt des in der Spec skizzierten `.[db]`. Badge im README.
- **Frisch-Clone-Probe** (Temp-Verzeichnis, `git clone`, frische venv, Python 3.14.4):

  ```
  377 passed in 2.77s
  All checks passed!            (ruff check .)
  53 files already formatted    (ruff format --check .)
  Success: no issues found in 53 source files   (mypy)
  ```

  **Ehrlich:** Das 3.12-Bein der Matrix war lokal nicht prüfbar (Server hat nur 3.14), und ein nackter GitHub-Runner ist erst der erste echte CI-Lauf nach dem Public-Schalter — steht als Checklisten-Schritt 2.
- **PyPI vorbereitet, nicht hochgeladen:** `python -m build` neu gebaut (dist/ von 011 ersetzt — reproduzierbares Build-Artefakt, alter Stand via git-History des Quellcodes jederzeit re-erzeugbar), `twine check dist/*` **PASSED** (sdist + wheel). Classifier `3.14` ergänzt (CI testet es, Server läuft darauf). Anleitung: `docs/pypi-release.md`. README-Umstellung auf `pipx install eigenlag` liegt als **nicht angewandter** Patch `launch/readme-pypi-install.patch` (`git apply --check` grün).
- **Launch-Texte** unter `launch/`, alle als "DRAFT — David redigiert" markiert: `reddit-post.md` (führt mit dem Sweep-Befund 30/29, Sauerteig in zwei Sätzen, 48-min-Konstanz samt Rückkopplungs-Caveat, Offenlegung + Link am Ende), `wikimedia-mail.md` (< 200 Wörter, Kanal-Alternativen Mailing-Liste/Phabricator mit Verifikations-Hinweis), `airflow-slack.md` (vier Sätze), `release-notes-v0.1.0.md` (analyze/check/demo, drei Limitations-Zeilen, Fallstudien-Link), `launch-checklist.md` (11 Schalter in Reihenfolge, je Zeile Wirkung + Risiko).
- **Verifikations-Artefakte:** `scan/013_launch/demo_en.txt`, `demo_de.txt` (über den Entry-Point gefahren).

```
$ pytest -q                     → 377 passed   (370 + 7 demo_test)
$ ruff check .                  → All checks passed!
$ ruff format --check .         → 53 files already formatted
$ mypy                          → Success: no issues found in 53 source files
```

**Was überrascht hat:** vhs läuft auf dem Server nur mit `VHS_NO_SANDBOX=true` (Host ohne nutzbare Chromium-Sandbox). Und die Terminal-Höhe des Tapes ist Rechenarbeit, kein Geschmack: 840 px ≈ 51 Zeilen sorgen dafür, dass nach dem Durchscrollen genau die Plan-Sektion als Endbild stehen bleibt (47 gewrappte Zeilen von Plan-Header bis Prompt bei 148 Spalten).

---

## 013a — Abnahme Launch-Kit durch den Orchestrator (2026-07-16)

**Abgenommen.** Gates nachgefahren (377 Tests, ruff, mypy). `eigenlag demo` selbst über den Entry-Point: 0,115 s, Kopfzeile deklariert das Beispiel, Fußzeile den nächsten Schritt. GIF geprüft, indem der Endframe per ffmpeg extrahiert und **angesehen** wurde: hält exakt auf der Plan-Sektion mit −43,18 % und dem Tragfähigkeits-Marker; 356 KB, 16,6 s. Der "foreign task"-Satz ist ersetzt ("whether and how to split is yours to judge").

**Die Launch-Texte sind der beste Teil der Session.** Gegen Davids Schreibregeln gelesen: ruhige ganze Sätze, keine Pointen-Stakkatos, Offenlegung der Autorenschaft, und — am wichtigsten — der Reddit-Post nimmt das stärkste Gegenargument (die Uhr-Rückkopplung) **selbst vorweg**, statt es einem Kommentar zu überlassen. Die Wikimedia-Mail schließt mit einem Dank für die offenen Daten statt mit einem Pitch. Die Checkliste hat je Schritt Wirkung und Risiko, inklusive des Details, dass der v0.1.0-Tag erst NACH der README-Umstellung gesetzt wird. Zahlen im Post gegen `case.md` verifiziert (453 Zeilen, 249 mit bekanntem Takt, 30/29).

**Ein Redigier-Hinweis für David, kein Fix-Loop:** In den Release-Notes steht "an acceleration plan that prices every possible change" — "prices" führt in die Irre (es gibt bewusst keine Geld-Rechnung im Tool, ADR-024) und "every possible change" überbehauptet (berechnet werden Kanten-Entfernungen und Task-Halbierungen). Vorschlag: "an acceleration plan that quantifies the structural changes that lower it".

**Ehrlich offen bleibt, was nur der Public-Schalter beweisen kann:** der erste echte CI-Lauf und das Python-3.12-Bein (Server hat nur 3.14; die Frisch-Clone-Probe bildet den Runner nach, beweist ihn aber nicht). Steht in der Checkliste als Schritt 2 mit Anweisung für den Rot-Fall.

**Damit ist die Roadmap bis 013 komplett und der Feedback-Meilenstein wartet nur noch auf Davids Schalter.** Reihenfolge: Repo public → CI prüfen → PyPI → README-Patch → Release → Ruhe → Wikimedia → Reddit → Slack → Woche-1-Reaktionsdienst.

---

## 013b — Externes Text-Review eingearbeitet (Gemini via David, 2026-07-16)

David hat die fünf Außentexte extern korrekturlesen lassen. Triage durch den Orchestrator, Einarbeitung direkt (von David für diese Runde autorisiert):

**Übernommen (9 von 10):** die Deutschismen "holds runtime against" → "compares" (Reddit + README), "pays 48 minutes" → "pays a 48-minute penalty" bzw. "incurs a constant 48-minute delay" (Reddit/Slack), "assembled at call time" → "generated dynamically", "both halves" → "both parts" (abgewandelt), der Mail-Lesefluss ("with a mean duration of … against a"), "prices every possible change" → "quantifies the impact of each structural change" (mit der 013a-Korrektur der Überbehauptung zusammengeführt), "unclaimed reserve" → Headroom-Vokabular, "yours to judge" → "up to you".

**Abgelehnt (1 von 10), mit Begründung:** der Vorschlag "The core value isn't just the specific λ number, but the distinction it makes" — "core value" ist Marketing-Vokabular und die "isn't just X, but Y"-Antithese ist genau das Muster, das die Schreibregeln verbieten. Stattdessen eigene, leisere Umstellung: "The single λ value matters less than the distinction it draws."

**Kaskade, weil zwei Fundstellen im Produkt-Code lagen:** "yours to judge" und "unclaimed reserve" standen nicht nur in der Doku, sondern in `messages.py` (EN-Katalog) — also Report-Strings geändert, Test-Pin angepasst (377 passed), der README-Quickstart-Block aus einem frischen echten Lauf neu erzeugt und auf die kuratierte Länge zurückgeschnitten, und das GIF aus der `.tape` neu gerendert (654 KB, Endframe gesichtet: neue Formulierungen, Umbruch jetzt an der Wortgrenze). Genau für diese Reproduzierbarkeit war das tape-Skript gebaut.

Das Review-Paket (`launch/review-fuer-gemini.md`) ist wieder entfernt, sein Zweck ist erfüllt.

---

## 013b — GIF-Korrektur: sichtbares Scrollen (2026-07-16)

**Davids Befund war korrekt:** `eigenlag demo` rendert in ~0,1 s, im GIF erschien der komplette Report deshalb innerhalb eines Frames — 16,6 s Gesamtdauer, aber nur ~4 s wahrnehmbare Animation, danach Standbild. Fix: der Report wird im Tape zeilenweise mit 30 ms je Zeile ausgegeben (als gestellte Stelle im Tape-Kommentar deklariert, wie die pipx-Zeile), ~2 s sichtbares Scrollen, dann Halten auf der Plan-Sektion.

Zweiter Befund dabei: **vhs 0.11.0 ignoriert `Set Framerate` für GIF-Output** (Mini-Tape gemessen: 83 Frames / 3,32 s = 25 fps trotz `Framerate 10`). Bei 25 fps ist jeder Scroll-Frame ein Vollbild-Wechsel, das GIF lag bei 7,7 MB. Deshalb baut jetzt `launch/build-demo-gif.sh` in zwei Schritten: vhs, dann ffmpeg auf 10 fps mit neu gerechneter Palette (`dither=none`, Text bleibt scharf). Ergebnis: **2 156 895 Bytes (2,1 MB < 3 MB), 14,6 s, 146 Frames.** Mid-Scroll- und Endframe per ffmpeg extrahiert und gesichtet: Scrollen sichtbar, Endstand auf der Plan-Sektion mit −43,18 %.

Hinweis: Das GIF zeigt bereits die redigierten EN-Formulierungen aus Davids Review-Runde ("untapped headroom", "up to you"), die zum Zeitpunkt des Renderns im Working Tree lagen.

---

## 014 — Pre-Flight: Übersetzungen und letzte Prüfungen vor dem Public-Schalter (2026-07-21)

Implementer-Session zur Spec 014. Kein Checklisten-Schritt ausgeführt, keine Historie umgeschrieben, keine Zahl verändert. Vier Aufgaben.

**Aufgabe 1 — Übersetzungen DE→EN (Dateien ersetzt, Pfade und Werte erhalten).** Vier Dokumente, in Prioritätsreihenfolge:

- `wikimedia/case.md` — das tragende öffentliche Dokument. Alle acht Permalinks unverändert, beide Code-Blöcke (inkl. Wikimedias Original-Kommentar mit dem Tippfehler "one ofter the other") verbatim, alle 60 Inline-Code-Spans verbatim, jede Zahl nur ins englische Zahlformat gebracht (Dezimalpunkt statt Komma, Tausender-Komma statt Punkt: `400.132 s` → `400,132 s`).
- `wiki/math.md` — Formeln und der Prototyp-Bezug erhalten. Die Pseudocode-Blöcke enthalten deutsche Prosa (also→so, über→over, Ende→End, Arbeit→Work), die mitübersetzt wurde; der einzige Zahlen-Token in Inline-Code, der z-Wert `1,6449`, wurde locale-korrekt zu `1.6449`.
- `wiki/signals.md` — Semantik jeder Signal-Definition unverändert (C nur mit `execution_delta`/`execution_date_fn` als Cross-Run; die λ-Übersetzungstabelle aus ADR-020 vollständig).
- `wiki/index.md` — englischer Zweizeiler oben ("development notes in German, public reference in English"), mit Links auf die drei englischen Dateien. Rest bleibt deutsch.
- `CLAUDE.md` — Sprachregel-Zeile angepasst (die drei genannten Dateien englisch, Rest deutsch als Entwicklungsarchiv); Datei bleibt sonst deutsch.

**Verifikation (Pflicht, nicht Kür):** ein locale-bewusstes Skript (`scratchpad/numcheck.py`) extrahiert je Datei die Zahlen-Tokens aus der Prosa, kanonisiert sie zum numerischen Wert nach Quell-Locale (DE: Komma=Dezimal, Punkt=Tausender; EN umgekehrt) und vergleicht als Multimengen; Datums-/Zeit-Tokens werden vorher entfernt, weil ihre Umformatierung (01.07. → July 1) kein Wert ist. Zusätzlich URL- und Code-Block-Multimengen. Ergebnis, jeweils DE (git HEAD) gegen EN (Working Tree):

- `case.md`: URLs OK (8), Code-Blöcke OK (2), Inline-Spans OK (60), Prosa-Zahlen **OK (119 Tokens, 71 Werte, identische Multimenge)**.
- `math.md`: Prosa-Zahlen **OK (34 Tokens, 19 Werte)**. Code-/Inline-Diffs ausschließlich beabsichtigte Prosa-in-Formel-Übersetzung plus `1,6449`→`1.6449`; kein Ziffernwert geändert.
- `signals.md`: URLs OK (0), Code-Block OK (1), Prosa-Zahlen **OK (38 Tokens, 24 Werte)**. Zwei Inline-Diffs (Ende→end, ziel→target), reine Identifier-Übersetzung.

**README-Präzisierung (Folge aus Aufgabe 1, im Report ausgewiesen):** Die Kopfzeile "Development docs are in German (`wiki/`)" wurde durch die Übersetzung teilweise falsch (case/math/signals sind jetzt englisch). Eine Zeile korrigiert, wahrheitswahrend, kein Redigat an Davids Stimme. Kein Anker-Link brach (keine Launch-Text- oder README-Verlinkung zielt auf `#abschnitt` in den drei Dateien — vorab geprüft).

**Aufgabe 2 — Prüfungen (nur Report).** `gitleaks` war Davids Vorarbeit (52 Commits, no leaks). Commit-Messages durchgehend technisch/deutsch, keine Secrets, keine dritten Personen außer der öffentlich gedachten Wikimedia-Referenz; der im Log sichtbare Orchestrator/Implementer-Workflow geht bewusst öffentlich. `.claude/` enthält nur `settings.json`, untracked und in keinem Commit (`git log --all -- .claude/` leer), mit lokalen Pfaden und einem jukeep.com-Verweis — Empfehlung: per `.gitignore`-Zeile absichern. Alle in der Checkliste referenzierten Artefakte existieren und sind konsistent (`readme-pypi-install.patch` `git apply --check` sauber, Badge-URL = Workflow-Pfad, alle README-Relativlinks auflösbar).

**Aufgabe 3 — positioning.md nicht angefasst;** die 3–5 gegen David verwendbaren Stellen samt Empfehlung (dazu stehen) gingen als Report an David.

**Aufgabe 4 — launch-checklist.md konsolidiert:** Schritt 0 (Sicherheits-/Historien-Review, erledigt) eingefügt, Schritt-1-Prüfhinweis auf Schritt 0 verwiesen, vor Schritt 2 der CI-Badge-Auslöser (die Übersetzungs-Commits) ergänzt, positioning-Entscheidung als expliziter offener Entscheidungspunkt vor Schritt 1, DRAFT-Markierung entfernt (offen bleiben nur Davids Schalter und die positioning-Frage).

**Gates:** 377 passed, `ruff check` clean, `ruff format --check` 53 Files, `mypy` 29 Files ohne Befund (unverändert, da nur Markdown geändert).

---

## 014a — Abnahme Pre-Flight durch den Orchestrator (2026-07-21)

**Abgenommen.** Gates nachgefahren (377 Tests, ruff, mypy — nur Markdown berührt, wie behauptet). Prüfschwerpunkt war die fachliche Treue der englischen `case.md`, stichprobenartig gegen das deutsche Original: die drei heikelsten Stellen stehen sinngleich — der Fixpunkt-Satz ("not a balancing act, it is the steady state of a feedback-coupled system"), die ADR-019-Einordnung ("It proves the thesis" mit der expliziten Identitätsfunktions-Ehrlichkeit "λ = 3598.4 s means the mean run duration is 3598.4 s"), und die Zahlen inklusive locale-korrekter Tausender-Trennung (400,132 s). PromQL-Blöcke unangetastet. Checkliste: Schritt 0 dokumentiert, positioning-Entscheidungspunkt eingetragen, DRAFT entfernt.

**Die eigenmächtige README-Kopfzeilen-Korrektur der Session wird bestätigt** — die Zeile "Development docs are in German (wiki/)" wäre nach der Übersetzung falsch gewesen; das Geradeziehen einer durch die eigene Arbeit falsch gewordenen Aussage ist Pflicht (Regel: Doku folgt dem Stand), kein Übergriff auf Davids Stimme.

**Umgesetzt bei der Abnahme:** `.claude/` in `.gitignore` (Empfehlung der Session) — schützt gegen ein versehentliches `git add -A`, bevor das Repo öffentlich ist. Die Datei selbst war nie committet (von der Session per `git log --all -- .claude/` belegt).

**Offen bleibt genau eine Entscheidung, und sie liegt bei David:** positioning.md — dazu stehen oder nicht. Die Zitate-Liste der Session ist präzise (der "Haupt-Pitch"-Fund und der Kommentare-als-Messinstrument-Satz sind die zwei wirklich angreifbaren Stellen), die Empfehlung des Orchestrators bleibt: dazu stehen, aus den in Spec 014 genannten Gründen. Danach nur noch die Schalter.

---

## 014b — positioning-Entscheidung: bleibt unverändert (David, 2026-07-21)

David hat entschieden: `wiki/positioning.md` bleibt unverändert online, inklusive Historie. Damit ist der letzte offene Entscheidungspunkt vor den Schaltern geschlossen. Checkliste nachgezogen, die vorbereitete Thread-Antwort liegt am Ende der Checkliste. Es gibt nichts Offenes mehr außer Davids Schaltern (Schritte 1–11).

---

## Launch-Schritte 1–6 ausgeführt (David + Orchestrator, 2026-07-21)

Schritt für Schritt, je mit Verifikation: (1) Repo public (David). (2) CI war schon auf dem privaten Repo grün gelaufen — beide Matrix-Beine bestätigt, auch das lokal nie geprüfte 3.12. (3) PyPI-Upload nach Frisch-Build, twine check PASSED und Inhalts-Sichtung der Artefakte (nur das Package, kein scan/, kein data/); Gegenprobe: pip install eigenlag aus frischer venv, demo läuft. (4) README-Patch angewandt, GIF auf absolute URL (PyPI-Rendering). (5) Tag v0.1.0 auf den Stand nach der README-Umstellung, GitHub-Release mit den von David freigegebenen Notes. (6) About-Beschreibung und Topics per gh gesetzt.

Offen: Schritt 7 (Ruhetage), dann 8–10 (Mail, Reddit, Slack — Davids Versand nach Redigat) und 11 (Woche-1-Reaktionsdienst).
