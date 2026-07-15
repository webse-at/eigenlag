# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 010 — CI-Gate `eigenlag check --against REF` (2026-07-15)

**Das Gate steht.** `eigenlag check PFAD --against REF` vergleicht je DAG Punkt-λ und
Cross-Run-Kanten-Menge gegen einen Git-Stand (temporärer detached Worktree, Nutzer-Repo
bleibt unangetastet) und schreibt den PR-Kommentar als Markdown auf stdout bzw.
`--comment-file` (`--json` aus derselben Quelle). Exit-Codes 0/1/3, die 2 bleibt bei
`analyze`. Kein GitHub-API-Call; GitHub-Actions-Beispiel in `docs/ci-gate.md`.
Davor, als eigener Commit: die beiden Report-Korrekturen aus 009a (Null-Delta-Sammelzeile,
Schlusssatz), am Flaggschiff belegt (15 Rauschzeilen → 1 Sammelzeile, `scan/010_gate/`).

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 010; Artefakte in `scan/010_gate/`)

| Was | Ergebnis |
|---|---|
| Fixture-Historie v1→v2→v3 | v2 gegen v1: Exit 3, Kommentar nennt Kante mit `pipeline.py:10` + Signal; v3 gegen v2: Exit 0; unverändert: Exit 0 — alle über die pipx-Installation |
| Default-Regel wörtlich | Sekunden-Modus (`--assume-duration 2500`): λ = 5000 s > T = 3600 s → Exit 3; `--assume-duration 1500`: λ = 3000 s < T → Exit 0 trotz neuer Kante |
| Struktur-Modus (ADR-022) | ohne Dauern-Quelle: neue Kante, die einen Kreis schließt, bei sub-täglichem Takt löst aus; @daily besteht; Kommentar sagt „Task-Einheiten" |
| Schärfere Modi | `--fail-on-new-edge` (auch @daily → 3), `--max-increase 20` bei λ 200→300 s ohne neue Kante → 3, `--max-increase 100` → 0 |
| Selbst-Anwendung | `eigenlag check eigenlag --against HEAD` → Exit 0, „Keine DAGs in beiden Staenden", Repo unangetastet (`git status`/`worktree list` identisch) |
| Worktree-Hygiene | per Test gepinnt inkl. Exception-Fall: Worktree verschwindet immer |

### Verifiziert

- `pytest`: **344 passed** (34 neue: 26 Gate, 8 Report-Kompaktierung; zuerst rot, Beleg im Log)
- `ruff check`, `ruff format --check` (46 Files), `mypy eigenlag/` (22 Files) grün
- `pipx install --force .` gelaufen, alle 5 Gate-Läufe über den Entry-Point
- Pflicht-Dependencies weiterhin null (`dependencies = []`)

## Hinweise für nächste Session

- **Roadmap: 011 (Packaging, README, Sprachfassung)** ist der letzte Baustein vor dem
  öffentlichen Weg (Feedback-Meilenstein, `wiki/roadmap.md`): Repo öffentlich, Wikimedia-Sweep
  als Content, Wikimedia anschreiben. Die Zielgruppe liest englisch — README englisch, und die
  Report-Sprachfrage (EN-Fassung oder `--lang`) entscheidet die 011-Spec; `compose()`/`render()`
  und `compose_check`/`render_check` haben die Trennung dafür.
- **Für den Orchestrator zu prüfen (010):** die Struktur-Modus-Default-Regel (neue Kante
  schließt Kreis + sub-täglicher Takt statt „λ > T", das in Task-Einheiten nicht auswertbar
  ist). Begründung und Spannungs-Befund in ADR-022 und `wiki/log.md` (Session 010, „Was
  überrascht hat"); der Verdict-Block ist eine einzelne Stelle (`gate._dag_row`) mit
  gepinnten Tests, falls anders geschnitten werden soll.
- README sagt noch „Es gibt noch kein installierbares CLI" — seit 009 falsch, Korrektur ist
  Teil des 011-Auftrags (Packaging/README ausdrücklich dorthin verschoben).
- `data/airflow-home/` (gitignored, Airflow-3.3.0-Test-DB aus 009) und `.venv-airflow/`
  liegen weiter; für 011 vermutlich unnötig, wegwerfbar.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten.

## Was David entscheiden muss

1. Nichts Blockierendes im Code. Nach 011 steht die eigentliche Entscheidung an: wann das
   Repo öffentlich geht und in welcher Reihenfolge Reddit-Post / Airflow-Slack / Wikimedia-Kontakt.
