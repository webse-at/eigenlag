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
