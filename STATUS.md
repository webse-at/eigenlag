# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 002 — Scanner: AST-Analyse (2026-07-14)

**Die Analyse-Schicht steht und trägt auf echtem Code.** Sie klont, parst, ordnet Signale dem richtigen DAG zu und protokolliert alles, was sie nicht sicher entscheiden kann, statt es zu raten. Der große Lauf, das CSV und `report.md` sind Session 003.

### Was liegt

- `scanner/clone.py` — `git clone --depth 1 --single-branch` in `data/repos/<org>__<repo>/`, 120 s Timeout, vorhandene Clones werden nicht neu gezogen, Fehler gehen strukturiert an den Aufrufer.
- `scanner/schedule.py` — `subdaily` / `daily_or_slower` / `none` / `dataset_triggered` / `unknown`. Cron wird gerechnet (kleinste Distanz zweier Feuerzeitpunkte über fünf Jahre), nicht am String geraten. Ohne `croniter` (ADR-010).
- `scanner/analyze.py` — DAG-Erkennung (`DAG(...)`, `with DAG(...) as x`, `@dag`), DAG-Scoping über Block, Variable und `dag=`-Argument, `default_args`-Auflösung aus Modul-Dict-Literalen, Signale A bis D und F, Task-Factories nach ADR-009 getrennt gezählt.
- `scanner/analyze_dbt.py` — Signal E, nur wenn `materialized='incremental'` **und** `is_incremental()` im kommentarfreien SQL. Materialisierung aus `{{ config() }}`, `schema.yml` oder `dbt_project.yml`, in dieser Reihenfolge.
- `scanner/fixtures/` — zwei nachgebaute Repos: zwei DAGs in einem File, `depends_on_past=False`, Signal im Kommentar und im Docstring, Sensor mit und ohne `execution_delta`, `default_args` aufgelöst und unauflösbar, `@dag`, ambiger Task, `SyntaxError`, Factory-Modul, dbt-Models mit allen vier Kombinationen.
- `pyproject.toml` — `pyyaml` als Scanner-Extra, Fixtures aus `ruff` und `mypy` genommen, `testpaths` gegen den Clone-Cache gepinnt.

### Was verifiziert wurde

- `pytest`: **141 passed** (105 im Scanner, 79 davon neu). `ruff check`, `ruff format --check`, `mypy` grün.
- **Rauchtest über 40 echte Kandidaten:** 352 DAGs, 4 Risiko-Kandidaten in einem Repo, 3 `SyntaxError` sauber protokolliert, 8 `unresolved_default_args`, 0 Abstürze, 0 Clone-Fehler. Alle vier Risiko-Belege von Hand im Clone nachgeschlagen, alle echt (`'depends_on_past': True` in `default_args`).
- **ADR-009 reproduziert:** `navikt/team_familie_airflow_dags` meldet Factory-Signale in `operators/kafka_operators.py:32` und `:33` — dieselben Zeilen wie die Handprüfung bei der Abnahme von 001. Das Repo hat 33 DAGs und null DAG-scoped Signale; ohne die Factory-Regel wäre es als signalfrei durchgelaufen.

### Nächster Schritt

**003 — Scan-Lauf und Report.** Spec liegt unter `cc-sessions/003_offen-scan-run-report.md`. Sie kann `clone.ensure_clone`, `analyze.analyze_repo` und `analyze_dbt.analyze_dbt_repo` direkt über `data/candidates.jsonl` laufen lassen.

## Hinweise für nächste Session

### Neue Entscheidungen

- **ADR-010** — Cron wird ohne `croniter` gerechnet. Dafür `pyyaml` als Scanner-Extra (dbt-YAML). Der `eigenlag`-Kern bleibt bei `dependencies = []`. Die seit Session 000 offene Dependency-Frage ist damit geschlossen.
- **ADR-011** — Signal F zählt in den `*_success`-Varianten als **starkes** Signal und damit in die Risiko-Quote. `signals.md` widersprach sich an dieser Stelle selbst und ist korrigiert.

### Was der Orchestrator prüfen soll

1. **Die Risiko-Quote der Stichprobe ist niedrig: 4 von 352 DAGs (1,1 %), 1 von 40 Repos.** Das ist die Zahl, an der Phase 1 hängt. Zwei Erklärungen sind noch nicht getrennt: (a) Cross-Run-Signale sitzen tatsächlich fast immer in täglichen DAGs, dann ist die These "Schedule zu schnell für den Kreis" seltener als gedacht; (b) die ersten 40 Zeilen von `candidates.jsonl` sind die Treffer der ersten Query und damit nicht repräsentativ. Session 003 löst das über den vollen Lauf. **Der Report muss die Quote unabhängig davon aushalten, auch wenn sie klein bleibt** — die Alternative wäre, die Definition zu lockern, und genau das verbietet ADR-005.
2. **`unresolved_default_args` läuft mit 8 Fällen auf 40 Repos.** Wenn diese Quote im vollen Lauf hoch bleibt, ist sie eine ausweisbare Untergrenze mehr, neben den Factories. Sie gehört als Zahl in `report.md`, nicht in eine Fußnote.
3. **Sub-tägliche DAGs sind mit 59 von 352 (17 %) häufiger als erwartet.** Die Signale sitzen nur woanders. Das ist ein Befund, kein Fehler, aber er verdient im Report einen Satz.

### Offene Entscheidungen

1. **`pipx` ist nicht installiert.** Wird für Session 009 gebraucht.
2. **`numpy` wird im Kern weiterhin nicht gebraucht**, erst bei Monte Carlo (Session 006).

### Ungelöste Fragen

- Was passiert, wenn die Risiko-Quote nach dem vollen Lauf klein bleibt. Die Stichprobe deutet in diese Richtung. Der Marktbeweis müsste dann anders formuliert werden: nicht "so viele Pipelines sind gefährdet", sondern "so viele Pipelines haben einen Kreis, den niemand kennt, und für keinen einzigen ist λ bekannt". Das ist die ehrlichere und vermutlich auch die stärkere Aussage, aber sie ist eine andere.
