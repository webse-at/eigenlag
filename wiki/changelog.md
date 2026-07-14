# Changelog

Feature-Historie. Ein Eintrag pro abgeschlossenem Feature, nicht pro Commit.

## Unreleased

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
