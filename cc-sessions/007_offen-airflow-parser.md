# Session 007 — Airflow-Parser (Phase 2) — ENTWURF

> **Status: Entwurf.** Abgelegt von der Implementer-Session 006 (Pflichtschritt 12), weil noch
> kein Spec des Orchestrators lag. Inhalt ist die Roadmap-Zeile 007 plus die offenen Punkte aus
> der Abnahme 004 — keine eigenen Entscheidungen. Der Orchestrator schärft oder ersetzt diesen
> Entwurf vor Session-Start.

## Auftrag (aus `wiki/roadmap.md`, Phase 2, Zeile 007)

Airflow-Parser per AST: aus einem DAG-File die `Pipeline` für den Mathe-Kern bauen.

- Tasks erkennen (Operatoren, `@task`), Kanten aus `>>`/`<<` und
  `set_upstream`/`set_downstream`
- Cross-Run-Signale nach `wiki/signals.md` (A bis G, Stand nach ADR-018) in `CrossEdge`
  übersetzen, inklusive Perioden-Versatz (ADR-006)
- Schedule-Klassifikation und Takt T über `scanner/schedule.py` (`period_seconds`)
- Offen aus Abnahme 004: Wiederholung des Karp/Howard/Brute-Force-Vergleichs auf echten
  geparsten DAGs, nicht nur auf der Demo-Pipeline

## Abhängigkeiten

004 (Mathe-Kern, liegt) und 006 (Signal-Definitionen final, liegt).

## Vorher lesen

`wiki/index.md`, `wiki/signals.md`, `wiki/math.md`, `wiki/architecture.md`, ADR-006, ADR-007,
ADR-018, `eigenlag/model.py`, `scanner/analyze.py` (Signal-Erkennung, die der Parser teilt
oder bewusst nicht teilt — das ist eine der Entscheidungen, die der Orchestrator treffen muss).
