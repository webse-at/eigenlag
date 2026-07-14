# Changelog

Feature-Historie. Ein Eintrag pro abgeschlossenem Feature, nicht pro Commit.

## Unreleased

- **2026-07-14** — **Mathe-Kern fertig** (Session 004). `eigenlag/model.py` und `eigenlag/maxplus.py`: Kondensation auf die Cross-Run-Knoten, Karp und Howard als unabhängige Verfahren für λ, Howard liefert zusätzlich den kritischen Kreis, dazu Drift, Drift-Simulation und Critical Path. Neu gegenüber dem Prototyp ist der Perioden-Versatz `CrossEdge.periods` (ADR-006). 35 Tests grün, λ = 4.40 h gegen den Prototyp reproduziert. Keine Laufzeit-Dependency.
- **2026-07-13** — Projekt aufgesetzt. Wiki, CLAUDE.md, STATUS.md, Session-Specs 001 bis 004. Referenz-Prototyp verifiziert (λ = 4.40 h reproduziert und hergeleitet).
