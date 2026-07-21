# Changelog

Feature-Historie. Ein Eintrag pro abgeschlossenem Feature, nicht pro Commit.

## Unreleased

### Session 010 — CI-Gate `eigenlag check` (2026-07-15)

- `eigenlag/gate.py` + CLI-Befehl `check`: `eigenlag check PFAD --against REF` vergleicht je DAG Punkt-λ und Cross-Run-Kanten-Menge des Arbeitsstands gegen einen Git-Stand. Vergleichsstand aus temporärem detached Worktree (read-only, restlos entfernt auch bei Exceptions, kein Checkout im Nutzer-Tree). Exit-Codes 0 bestanden / 1 Bedienfehler / 3 ausgelöst; die 2 bleibt bei `analyze`
- Default-Fail-Regel nach Auftrag: neue Cross-Run-Kante **und** λ_nachher > T (Sekunden-Modus via `--db`/`--assume-duration`); im Struktur-Modus (CI-Default, uniforme Dauern 1.0) löst eine neue Kante aus, die einen Kreis schließt, bei sub-täglichem Takt (ADR-022). Schärfere Modi: `--fail-on-new-edge`, `--max-increase PROZENT`
- PR-Kommentar (Markdown, stdout bzw. `--comment-file`, `--json` aus derselben Quelle): Urteil zuerst, je betroffenem DAG λ vorher → nachher mit Einheit, T mit Quelle, die auslösende Kante mit Datei:Zeile und Signal-Art, Kreis kondensiert und aufgelöst (ADR-002), Behebungs-Hinweis aus dem What-if-Ranking, Modellgrenzen in zwei Sätzen. Kein GitHub-API-Call, niemals — GitHub-Actions-Beispiel in `docs/ci-gate.md`
- Report-Korrekturen aus 009a: What-if-Zeilen mit ±0 werden zur Sammelzeile kompaktiert (Kreis-Gleichstände vs. Kanten außerhalb des kritischen Kreises; `--json` behält alle Zeilen, neues Feld `auf_kreis`), Schlusssatz beschreibt jetzt das tatsächliche Verhalten. Am Flaggschiff belegt: 15 Rauschzeilen → 1 Sammelzeile (`scan/010_gate/`)
- ADR-022 (Punkt-λ als Gate-Metrik, Monte Carlo nie gegen Schwellen), `select_dags` nach `parse_airflow.py`, Kreis-Block als `report.cycle_report()` geteilt
- 34 neue Tests (344 im Repo), darunter Fixture-Repos mit echter Git-Historie und parametrisierte Exit-Code-Matrix; Kern weiter ohne Laufzeit-Dependencies

### Session 009 — CLI `eigenlag analyze` (2026-07-15)

- `eigenlag/cli.py`: `eigenlag analyze PFAD` (argparse, Entry-Point via `[project.scripts]`, per `pipx install .` verprobt). Quellen `--db` / `--rest --rest-token` / `--assume-duration` mischbar wie in 008; `--dag-id`, `--statistic`, `--since`, `--period`, `--samples`, `--what-if` (wiederholbar: `task=NAME:SEKUNDEN`, `drop-edge=SRC->DST`), `--json`. Exit-Codes: 0 analysiert (auch instabil), 1 Bedienfehler, 2 kein analysierbarer DAG
- `eigenlag/report.py`: der deutsche Report — Urteil zuerst (stabil / an der Grenze mit Rückkopplungs-Hinweis / instabil mit Drift und Zeit bis 1 h Rückstand / „nicht anwendbar: keine Cross-Run-Kante" statt λ = 0), Kreis doppelt mit Signal und Datei:Zeile je Segment (ADR-002), Monte Carlo mit Pendel-Satz, What-if-Ranking (Standard-Szenarien automatisch, „bringt exakt null"), Pflicht-Warnblock (nie abschaltbar, inkl. F-Divergenz-Erklärung nach ADR-020) und Modellgrenzen-Fußzeile. `compose()` liefert die stabilen JSON-Keys für das 010-Gate, `render()` den Text aus derselben Quelle
- `eigenlag/montecarlo.py`: stdlib-Monte-Carlo (analytischer Lognormal-Fit aus p50/p95, Kondensation pro Sample, fester Seed, Tasks ohne Varianz-Basis konstant); 1000 Samples auf der Demo-Pipeline in 0,05 s, `numpy` bleibt draußen
- ADR-021 umgesetzt: Selbst-Referenz-Sensor (`external_dag_id == eigene dag_id`, `execution_delta = n × T`) wird Cross-Kante im eigenen Namespace; 007-Graph-Check neu: 4836/4836 Karp = Howard
- Postgres-Integrationsbeleg (offener Punkt aus 008): `percentile_cont`-Pfad == Python-Aggregation auf denselben Fixture-Zeilen, Wegwerf-Container `postgres:16`
- 38 neue Tests (312 im Repo), Kern weiter ohne Laufzeit-Dependencies

### Session 008 — Dauern-Schicht (2026-07-15)

- `eigenlag/durations.py`: drei Quellen, eine Ausgabeform (`TaskStats`: p50/p95/mean/n/operator/is_sensor) — `from_metadata_db` (sqlalchemy lazy, neues Extra `eigenlag[db]`, PostgreSQL aggregiert per `percentile_cont`, sonst Python), `from_rest` (urllib, Paginierung, max. 2 Requests/s, Seiten-Deckel mit Warnung), `assume(seconds)`. `pick`/`resolve` bauen das `durations`-Mapping für `to_pipeline`; Mischbetrieb und Mindest-Stichprobe (n < 5) fallen je Task mit Warnung auf den Assume-Wert
- `eigenlag/analyze.py`: `analyze(path, stats, statistic, fallback)` — parsen, Dauern heiraten, kondensieren, Howard; **das erste λ in Sekunden**. Sensor auf dem kritischen Kreis erzeugt die Pflicht-Warnung `sensor_im_kritischen_kreis` (markieren statt herausrechnen, `math.md` Abschnitt 9)
- Schema-Verifikation gegen echtes Airflow 3.3.0 standalone (`.venv-airflow`, Python 3.12.13): `task_instance`-Spalten, `duration`-Sekunden, TaskGroup-Prefix bestätigt; **Airflow 3 hat `/api/v1` und Basic Auth entfernt** → `api_version`-Parameter (Default v2 + JWT-Token, v1 für Airflow 2), DB- und REST-Pfad liefern auf denselben Läufen identische Statistik. Belege in `wiki/log.md`
- Sensor-Nachlauf der 14 offenen 007a-Fälle über ganze Repos (`scanner/sensor_followup.py`, `scan/008_sensor/nachlauf.csv`): 1 modellierbar (periods=1, kein Kreis), 11 weiterhin nicht (mit Grund), 2 Ziel nicht im Repo — **kein `periods > 1` in freier Wildbahn**, ADR-006 bleibt test-belegt; Selbst-Referenz-Sensor als ADR-Kandidat protokolliert
- 18 neue Tests (274 im Repo), Kern weiter ohne Laufzeit-Dependencies

### Session 007 — Airflow-Parser (2026-07-14)

- `eigenlag/parse_airflow.py`: AST-Parser DAG-File → `ParsedDag` (Tasks, Intra-Kanten, Cross-Kanten mit Herkunft, Warnungen). Task-Erkennung: Operatoren mit statischem `task_id`, `@task`/`@task.*`, `.partial().expand()` (eine Task, Warnung `task_mapping`); Kanten: `>>`/`<<` gekettet und mit Listen, `set_upstream`/`set_downstream`, `chain(...)`, TaskGroups mit Prefix-Namespace. Nicht statisch Auflösbares wird Warnung mit Datei und Zeile, nie geraten
- Übersetzungstabelle Signal → λ-Kante komplett als Tests zuerst (rot → grün im Log belegt): A Selbstkante (default_args vererbt, Operator überschreibt), B zusätzlich direkte Downstreams, C nur bei Ziel im Parse-Satz + gleichem T + ganzzahligem `delta/T` (einzige Kante mit `periods > 1`), D/F ohne Kante als Befund (F: ADR-020), G Senke×Quelle
- Import-Beleg: Files, deren `DAG` nicht aus `airflow` importiert ist, werden nicht geparst (`dag_not_airflow`) — verhindert den 330-Zeilen-Fehler aus 006 im Produkt
- `to_pipeline(dags, durations=1.0)`: Struktur + Dauern → `Pipeline`, Knoten namespaced `dag_id.task_id`; Dauern kommen in Session 008
- `eigenlag/schedule.py` (Umzug aus `scanner/`), Scanner importiert aus dem Package; `scanner/parse_consistency_test.py` pinnt Signal-Arten-Gleichheit Parser ↔ Scanner auf den Fixtures
- Korpus-Validierung (`scanner/parse_corpus.py`, `scan/007_parse/`): 626 Kandidaten-Files, 4892 DAGs, 0 Syntax-Fehler; Karp = Howard auf allen 4836 kondensierten Graphen, 4827 zusätzlich per Brute-Force; 3 Konsistenz-Abweichungen (alle = dokumentierte Import-Beleg-Differenz); keine statisch modellierbare C-Kante im Korpus (34 Fälle, jede mit Grund)
- Teilpfad-Jagd (ADR-019): 129 Kern-Kandidaten-DAGs in 77 Repos mit λ < Critical Path bei uniformen Dauern; durchgerechneter Fall `udac_example_dag` (λ = 2 vs. CP = 6, `wait_for_downstream` in default_args) im Log
- ADR-020 (F: Marktzahl ja, λ-Kante nein), λ-Übersetzungstabelle in `signals.md`. 42 neue Tests (256 im Repo), Kern weiter ohne Laufzeit-Dependencies

### Session 006 — Re-Scan mit Zwei-Klassen-Risiko, Fall-Korrektur (2026-07-14)

- Voller Re-Scan über die 1692 gecachten Clones (1193 s, kein Neu-Klonen, State versioniert unter `data/scan_state_v2/`), Artefakte unter `scan/v2/`, die 003-Artefakte bleiben unverändert liegen
- `scanner/report.py`: Zwei-Klassen-Risiko nach ADR-018 (`risk_candidate` unverändert A–F, neu `risk_candidate_g_only`), Spalten `sig_g_max_active_runs` und `dag_id_missing`, Vorher/Nachher-Tabelle gegen 003, Offenlegung der Definitionsänderung im Report-Text; Permalinks URL-encodiert (Dateinamen mit `#` waren nicht nachschlagbar)
- Ergebnis: 51.789 DAGs (+363 durch ADR-015), Cross-Run (A–F) und Kern-Kandidaten **mengen-identisch** mit 003 (1303 / 176), neu 473 G-only-Kandidaten in 159 Repos, 4952 DAGs ohne `dag_id`; dbt byte-identisch übernommen (3369). Die 005-Hypothese "Marktzahl steigt deutlich" ist gemessen widerlegt: der öffentliche Korpus kapselt seine DAG-Erzeugung kaum
- Drei 10er-Stichproben (Kern, G-only, signalfrei) mit 0 Falsch-Positiven und 0 Falsch-Negativen in `scan/v2/sample_verification.md`, Delta-Zuordnung mit zwei 5er-Stichproben (+422 Konstruktor-Aufrufstellen, −59 Schablonen)
- `wikimedia/case.md` nach ADR-017 überarbeitet: der Sweep (30 DAGs über Takt, 29 driften nicht) ist die Überschrift, "1,6 Sekunden Reserve" gestrichen (der Wert ist der Fixpunkt des rückgekoppelten Systems, keine Marge), λ = Laufdauer auf DAG-Ebene explizit benannt, wcqs-Ausreißer-Absatz ergänzt; Messwerte unverändert. Die 005-Einträge in Log und Changelog bleiben als Historie stehen, die Richtigstellung steht in 005a und ADR-017
- ADR-018 in `wiki/decisions.md`, `signals.md` auf die Zwei-Klassen-Definition gebracht (Titel jetzt "A bis G")

### Session 003 — Scan-Lauf und Report (2026-07-14)

- `scanner/run.py`: voller Lauf über 1692 Kandidaten, resume-fähig, Clone-SHA im Permalink
- `scanner/report.py`: `scan_results.csv` (eine Zeile je DAG), `scan_factories.csv`, `scan_dbt.csv`, `report.md`
- `analyze.py`: `task_count` je DAG; Signal F auch als Callable-Parameter und Modul-Variable (ADR-013); `execution_delta=0` ist kein Signal mehr (ADR-014)
- Artefakte unter `scan/`, Stichproben in `scan/sample_verification.md`
- Ergebnis: 51.426 DAGs, 1303 mit Cross-Run, 176 Risiko-Kandidaten, 3369 dbt-Models mit Selbst-Kante. 78 % der Risiko-Kandidaten sind Beispiel-Code.

- **2026-07-14** — **Scanner-AST-Analyse fertig** (Session 002). `scanner/clone.py` (flache Clones, Disk-Cache, 120 s Timeout), `scanner/schedule.py` (Cron gerechnet statt geraten, ohne `croniter`, ADR-010), `scanner/analyze.py` (DAG-Erkennung, DAG-Scoping über `with`, Variable und `@dag`, `default_args`-Auflösung, Signale A bis D und F, Task-Factories nach ADR-009), `scanner/analyze_dbt.py` (Signal E, Kommentare vorher entfernt). Fixtures unter `scanner/fixtures/` mit jeder Falle. Rauchtest über 40 echte Repos: 352 DAGs, 4 Risiko-Kandidaten, 0 Abstürze. 79 neue Tests (141 im Repo). Neue Scanner-Dependency: `pyyaml` (Extra `scanner`), Kern bleibt abhängigkeitsfrei.
- **2026-07-14** — **Scanner-Harvest fertig** (Session 001). `scanner/harvest.py`: sechs Code-Search-Queries gegen `/search/code`, proaktive Drosselung beider Kontingente, Filter mit protokolliertem Grund je verworfenem Repo, Resume über `hits.jsonl` und `harvest_state.json` (ADR-008). Erster Lauf: 2095 Repos bewertet, 1692 Kandidaten (1328 Airflow, 364 dbt), 403 verworfen. 26 Scanner-Tests grün (62 im Repo).
- **2026-07-14** — **Mathe-Kern fertig** (Session 004). `eigenlag/model.py` und `eigenlag/maxplus.py`: Kondensation auf die Cross-Run-Knoten, Karp und Howard als unabhängige Verfahren für λ, Howard liefert zusätzlich den kritischen Kreis, dazu Drift, Drift-Simulation und Critical Path. Neu gegenüber dem Prototyp ist der Perioden-Versatz `CrossEdge.periods` (ADR-006). 35 Tests grün, λ = 4.40 h gegen den Prototyp reproduziert. Keine Laufzeit-Dependency.
- **2026-07-13** — Projekt aufgesetzt. Wiki, CLAUDE.md, STATUS.md, Session-Specs 001 bis 004. Referenz-Prototyp verifiziert (λ = 4.40 h reproduziert und hergeleitet).

### Session 005 — Der Wikimedia-Fall (2026-07-14)

- `wikimedia/fetch.py`: Prometheus über Wikimedias Grafana-Proxy, Cache-first, Rate-Limit, Fehlerprotokoll, Block-Zerlegung für `query_range`
- `wikimedia/runs.py`: Läufe aus der Gauge rekonstruieren (Wertwechsel je Serie), Fenster ohne Metrik-Lücke, Statistik (ADR-017)
- `wikimedia/case.py`: Scan + Messung + λ über den Kern + Sweep über alle (DAG, Instanz)-Paare der Organisation
- `wikimedia/case.md`: der Fall, mit PromQL und Permalink zu jeder Zahl. `wikimedia/wikimedia_dags.csv`: 453 Zeilen
- `scanner/wrappers.py`: repo-eigene DAG-Konstruktoren (ADR-015). Wikimedia: 71 → 345 DAGs, 0 → 13 Cross-Run-Signale
- `scanner/analyze.py`: Signal G, `max_active_runs=1` (ADR-016). Wikimedia: 13 → 68 Cross-Run-Signale, 3 → 8 Risiko-Kandidaten
- `scanner/schedule.py`: `period_seconds`, der Takt T aus dem Schedule-Ausdruck gerechnet
- Ergebnis: `wdqs_streaming_updater_reconcile_hourly` läuft mit λ = 3598,4 s gegen T = 3600 s, also 1,6 s Reserve, bei 48 Minuten dauerhafter Verspätung. 67 neue Tests (207 im Repo).

### Session 011 — Zweisprachig, installierbar, veröffentlichungsreif (2026-07-15)

- `eigenlag/messages.py`: EN/DE-Nachrichten-Kataloge, `fmt`/`dur`/`perioden`/`scenario_label`/`t`; Vollständigkeits-Test (`messages_test.py`)
- `report.render(d, lang)` / `gate.render_check(d, lang)` katalogbasiert zweisprachig; `--json` sprachneutral und byte-identisch (`i18n_test.py`); sprachneutrale Struktur-Felder in `compose`/`compose_check` (ADR-023)
- CLI `--lang en|de` (Default en) an `analyze` und `check`; argparse-Hilfe + Fehlermeldungen einsprachig englisch (ADR-023, Spec-Punkt 4)
- README.md englisch neu (Quickstart aus echtem Lauf, CI-Gate, Wikimedia-Beleg, Limitations); `docs/ci-gate.md` englisch
- Packaging: `pyproject.toml` (MIT/PEP 639, classifiers, keywords, urls), `LICENSE`, `dist/` gitignored; `python -m build` + Wheel in frischer venv verifiziert
- 356 Tests grün (Pflicht-Dependencies weiterhin null)

### Session 012 — Beschleunigungsplan: aus der Diagnose wird das Produkt (2026-07-16)

- `eigenlag/plan.py` neu: `build_plan` reichert die What-if-Zeilen an (Kanten-Art A–G/dbt-E, Katalog-Schlüssel, λ_neu, Delta absolut und Prozent, `macht_tragfaehig`, verdict-abhängige Gewinn-Felder), Paar-Rechnung der drei wirksamsten Aktionen bei instabilem Takt ohne rettende Einzel-Aktion; reine, sprachneutrale Funktion (`plan_test.py`, 16 Pins)
- `eigenlag/messages.py`: Behebungs-Katalog `plan_fix_*` je Kanten-Art in EN und DE (Muster-Wissen, nie Garantie) plus die Plan-Render-Keys; Vollständigkeit per Test erzwungen
- `report._plan_text` ersetzt `_what_if_text`; Report-Reihenfolge Urteil → Kreis → **Beschleunigungsplan** → Monte Carlo → Warnungen; `--json` `plan`-Key **additiv**, `what_if` eingefroren, EN/DE byte-identisch (ADR-024)
- Zwei Gewinn-Formen: instabil „makes your current schedule sustainable" ⇔ λ_neu < T plus weggeräumte Drift; stabil Headroom (Läufe/Tag mehr, „bis zu" T − λ frischer) ohne erfundene Marge
- Demo als Marketing-Artefakt (Quality-Gate-Kante rettet den Takt, GPU-Upgrade nicht), Flaggschiff EN+DE, synthetischer Paar-Fall, README-Quickstart auf echten Lauf umgestellt; 370 Tests grün, Pflicht-Dependencies weiterhin null

### Session 013 — Launch-Kit (2026-07-16)

- `eigenlag demo` neu: eingebauter Beispiel-Report der Prototyp-Pipeline (EN/DE, < 1 s, kein Netz, keine Dateien), Kopfzeile deklariert das Beispiel, Fußzeile den nächsten Schritt; Fixture DUR/INTRA/CROSS zog als Single Source nach `eigenlag/demo.py`
- `plan_fix_task_halved` umformuliert (EN/DE): der "foreign task"-Deutschismus ist raus, die Schnitt-Entscheidung bleibt beim Leser (012a-Feinschliff)
- `launch/demo.tape` + `assets/demo.gif` (vhs, 356 KB, 16,6 s, reproduzierbar), im README oben eingebettet
- `.github/workflows/ci.yml`: Matrix Python 3.12/3.14, pytest/ruff/mypy, identisch zur dokumentierten Frisch-Clone-Probe; CI-Badge im README
- PyPI vorbereitet: `twine check` PASSED, Classifier 3.14, `docs/pypi-release.md`, README-Install-Umstellung als nicht angewandter Patch `launch/readme-pypi-install.patch`
- Launch-Texte als DRAFTs unter `launch/`: Reddit-Post, Wikimedia-Mail, Airflow-Slack, Release-Notes v0.1.0, Schalter-Checkliste
- 377 Tests grün (Pflicht-Dependencies weiterhin null); veröffentlicht hat die Session nichts

### Session 014 — Pre-Flight: öffentliche Referenz auf Englisch (2026-07-21)

- `wikimedia/case.md`, `wiki/math.md`, `wiki/signals.md` DE→EN übersetzt (Werte, Permalinks, Code-Blöcke unverändert; Zahlen locale-korrekt umformatiert), verifiziert per Zahlen-/URL-/Code-Multimengen-Diff (case: 119 Zahlen-Tokens identisch; math: 34; signals: 38)
- `wiki/index.md` englischer Sprach-Zweizeiler oben; `CLAUDE.md` Sprachregel präzisiert; README-Kopfzeile "development docs" wahrheitswahrend nachgezogen
- `launch/launch-checklist.md` konsolidiert: Schritt 0 (Sicherheits-/Historien-Review, erledigt), positioning-Entscheidungspunkt vor Schritt 1, CI-Badge-Auslöser vor Schritt 2, DRAFT entfernt
- Prüfungen (Report an David): Commit-Historie, `.claude/`, Checklisten-Artefakte, positioning-Zitate; nichts an Code, Historie oder Zahlen geändert
- 377 Tests grün, ruff/mypy sauber (nur Markdown berührt)
