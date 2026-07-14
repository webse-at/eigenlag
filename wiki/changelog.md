# Changelog

Feature-Historie. Ein Eintrag pro abgeschlossenem Feature, nicht pro Commit.

## Unreleased

- **2026-07-14** — **Scanner-Harvest fertig** (Session 001). `scanner/harvest.py`: sechs Code-Search-Queries gegen `/search/code`, proaktive Drosselung beider Kontingente, Filter mit protokolliertem Grund je verworfenem Repo, Resume über `hits.jsonl` und `harvest_state.json` (ADR-008). Erster Lauf: 2095 Repos bewertet, 1692 Kandidaten (1328 Airflow, 364 dbt), 403 verworfen. 26 Scanner-Tests grün (62 im Repo).
- **2026-07-14** — **Mathe-Kern fertig** (Session 004). `eigenlag/model.py` und `eigenlag/maxplus.py`: Kondensation auf die Cross-Run-Knoten, Karp und Howard als unabhängige Verfahren für λ, Howard liefert zusätzlich den kritischen Kreis, dazu Drift, Drift-Simulation und Critical Path. Neu gegenüber dem Prototyp ist der Perioden-Versatz `CrossEdge.periods` (ADR-006). 35 Tests grün, λ = 4.40 h gegen den Prototyp reproduziert. Keine Laufzeit-Dependency.
- **2026-07-13** — Projekt aufgesetzt. Wiki, CLAUDE.md, STATUS.md, Session-Specs 001 bis 004. Referenz-Prototyp verifiziert (λ = 4.40 h reproduziert und hergeleitet).
