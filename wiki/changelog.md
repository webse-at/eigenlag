# Changelog

Feature-Historie. Ein Eintrag pro abgeschlossenem Feature, nicht pro Commit.

## Unreleased

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
