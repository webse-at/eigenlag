# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 004 — Mathe-Kern (2026-07-14)

**Der Kern steht und rechnet.** `eigenlag/` ist ein installierbares Package ohne Laufzeit-Dependency. Phase 1 (Scanner, Specs 001 bis 003) ist unangetastet.

### Was liegt

- `eigenlag/model.py` — `Pipeline`, `CrossEdge`, Toposort. Validierung an der Systemgrenze.
- `eigenlag/maxplus.py` — `condense`, `karp`, `howard`, `drift`, `simulate`, `critical_path`.
- `eigenlag/model_test.py`, `eigenlag/maxplus_test.py` — 35 Tests.
- `pyproject.toml` — pytest (`*_test.py` neben dem Source-File), ruff (line-length 100), mypy strict.
- `.venv/` mit pytest, ruff, mypy. Python 3.14.4.

### Was verifiziert wurde

- **λ = 4.40 h** auf der Demo-Pipeline, deckungsgleich mit dem Prototyp. Karp und Howard stimmen auf allen acht Fixtures überein (ADR-003).
- **Kritischer Kreis** `monitor → monitor`, aufgelöst `core → features → retrain → score → monitor`. What-if: 3.60 / 2.50 / 3.85.
- **Drift** bei T = 3.0: gemessen 1.40 h/Lauf, analytisch λ - T = 1.40.
- **`periods` kommt in der Rechnung an.** Mutations-Test: zwingt man den Versatz in Karp und Howard auf 1, fallen genau die vier Perioden-Tests und kein anderer. Belege im Log, Eintrag 004.
- `pytest` 35 passed, `ruff check` und `ruff format --check` grün, `mypy eigenlag/` grün.

### Nächster Schritt

Nach Roadmap ist Phase 1 (Scanner) die Vorgabe: **001 — Scanner-Harvest**. Falls David den Kern weiterbauen will, ist **005 — Parser (Airflow/dbt)** die natürliche Fortsetzung, aber eine Spec dafür existiert noch nicht.

## Hinweise für nächste Session

### Neue Entscheidungen

- **ADR-006** — Zyklusmittel ist `Summe der Gewichte / Summe der periods`. Herleitung über die Zustandserweiterung (eine Kante mit Versatz n wird zu n Kanten der Länge 1). Karp rechnet auf dieser Expansion, Howard rechnet das Verhältnis nativ. Bewusst kein geteilter Code, damit der Kreuzvergleich aus ADR-003 seinen Wert behält.
- **ADR-007** — `karp` und `howard` geben `... | None` zurück, nicht `float`. Abweichung von der Signatur in Spec 004, begründet: kein Kreis heißt kein λ, und der Fall tritt nicht nur bei `cross == []` auf, sondern auch bei einer Cross-Kante ohne Rückweg.

### Offene Entscheidungen

1. **`numpy` wird im Kern nicht gebraucht.** Die kondensierte Matrix ist klein, reines Python reicht. Das Package hat aktuell null Laufzeit-Dependencies. Bei Monte Carlo (Session 006) ist `numpy` wieder das Mittel der Wahl, dann als echte Dependency eintragen.
2. **`croniter` als Scanner-Dependency** (aus Session 000 offen). Spec 002 erlaubt sie im Scanner, nicht im `eigenlag`-Package.
3. **`pipx` ist nicht installiert.** Wird für Session 009 gebraucht.

### Vom Orchestrator geprüft (2026-07-14) — Session 004 abgenommen

Tests, `ruff` und `mypy` unabhängig nachgefahren. Die acht Pins außerhalb der Session-Testdatei gegen den Prototyp nachgerechnet, alle deckungsgleich.

- **Howard-Risiko erledigt.** Der Einwand der Session war berechtigt: Karp und Howard stammen aus derselben Session, ein gemeinsamer Denkfehler in ADR-006 wäre unentdeckt geblieben. Gegenmaßnahme: ein drittes, absichtlich stumpfes Verfahren (Brute-Force über alle einfachen Kreise) als Referenz. **3000 Zufallsgraphen, null Abweichungen**, davon 529 kreislos, der `None`-Pfad aus ADR-007 wurde also wirklich durchlaufen. Liegt als `eigenlag/crosscheck_test.py` im Repo und läuft künftig bei jeder Änderung an Karp oder Howard mit. 36 Tests grün.
- **Restrisiko, ehrlich benannt:** getestet bis 5 Knoten. Der Vergleich auf echten, geparsten DAGs bleibt offen und gehört in die Parser-Session.
- **ADR-007 bestätigt.** Die Spec schrieb `-> float`, die Session liefert `-> float | None`. Die Spec war zu eng: der kreislose Fall tritt auch bei einer Cross-Kante ohne Rückweg auf. Richtig entschieden, die Spec war falsch, nicht die Implementierung.
- **`simulate` misst Makespan** (spätestes Task-Ende minus Release), der Prototyp misst einen bestimmten Sink-Task (`reports`). Auf der Demo identisch, bei mehreren Sinks nicht. Makespan ist die richtige Wahl für ein generisches Tool, aber **die Report-Schicht muss dem Nutzer sagen, was sie misst.** Gehört in die Spec der CLI-Session (007).

### Ungelöste Fragen

- Unverändert aus Session 000: die Repräsentativität der Scan-Stichprobe und die Frage, was passiert, wenn die Risiko-Quote klein ausfällt.
