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
