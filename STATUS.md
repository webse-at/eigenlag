# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 011 — Packaging, englisches README, Report-Sprachfassung (2026-07-15)

**Das Repo ist veröffentlichungsreif.** Englisch ist Report-Default, Deutsch bleibt
vollwertig unter `--lang de` (ADR-023). Lizenz **MIT**. Installierbar über `pipx`,
`python -m build` liefert sdist + wheel. Veröffentlicht wird nichts — Repo public
stellen, posten, Wikimedia anschreiben löst David aus.

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 011)

| Was | Ergebnis |
|---|---|
| Zweisprachigkeit | `eigenlag/messages.py`: EN/DE-Kataloge, keine i18n-Lib. `render`/`render_check` katalogbasiert, `--lang en|de` (Default en) an beiden Subcommands |
| `--json` | über beide Sprachen **byte-identisch** (per `diff -q` bei `analyze` und `check` belegt); Werte deutsch eingefroren, Keys unverändert |
| compose-Schnitt | sprachneutrale Struktur-Felder additiv ergänzt (`art`/`src`/`dst` je What-if-Zeile; `gruende_codes`/`behebung_code`/`hinweis_codes` im Gate), damit der Renderer den Text pro Sprache baut — ADR-023 |
| CLI | argparse-Hilfe + Fehlermeldungen + Quellen-Beschriftungen einsprachig englisch (Spec-Punkt 4) |
| README | englisch neu, Quickstart aus echtem Lauf (λ = 4000 s > T = 3600 s, Drift 400 s/Lauf, Mehr-Task-Kreis), Wikimedia-Sweep, Pflicht-Limitations |
| Packaging | `pyproject.toml` (MIT/PEP 639, classifiers, keywords, urls), `LICENSE`; `dist/` gitignored |

### Verifiziert

- `pytest`: **356 passed** (neu: `messages_test.py`, `i18n_test.py`; DE-Tests auf
  `--lang de`/`render(…, "de")` umgestellt, EN-Tests ergänzt). `ruff check`,
  `ruff format --check` (25 Files), `mypy eigenlag/` (25 Files) grün.
- `python -m build`: sdist + wheel, LICENSE eingebettet, METADATA `License-Expression: MIT`;
  Wheel in frischer venv installiert, `eigenlag --help` daraus englisch.
- `pipx install --force .`, dann über den Entry-Point: `analyze` EN + DE, `check` EN + DE
  (Exit 3), `--json` byte-identisch EN vs. DE.
- Pflicht-Dependencies weiterhin null (`dependencies = []`).

## Hinweise für nächste Session

- **Für den Orchestrator zu prüfen (011):** der bewusste Schnitt an „compose() wird nicht
  angefasst" (ADR-023). `--json` bleibt byte-identisch und die Keys stabil, aber es kamen
  sprachneutrale Struktur-Felder dazu — anders ist ein korrekter englischer Report nicht
  baubar. Wenn das anders geschnitten werden soll, ist die Stelle klar umgrenzt
  (`report._what_if`, `gate._dag_row`/`compose_check`).
- **Geflaggte Grenze (Folge-Ticket):** Die terse Diagnose-Details des Parser-/Dauern-Layers
  (z. B. „no measurement, 300 s", die `edge_dropped`-Gründe) sind einsprachig englisch, auch
  im deutschen Report. Die Report-Kernprosa (Urteil, Kreis, Monte Carlo, What-if, Sensor-Texte,
  Modellgrenzen) ist voll zweisprachig. Voll bilinguale Details bräuchten strukturierte
  Warnungen (`kind` + Parameter statt fertigem `detail`-String) — sauberes, kleines Folge-Ticket.
- `data/airflow-home/` und `.venv-airflow/` (beide gitignored) liegen weiter, wegwerfbar.
  `.claude/` ist untracked und wurde bewusst nicht committet.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten. **dbt-Parser** bleibt bis nach dem Feedback-Meilenstein vertagt.

## Was David entscheiden muss

1. Nichts Blockierendes im Code. Die eigentliche Entscheidung: wann das Repo public geht und
   in welcher Reihenfolge Reddit-Post / Airflow-Slack / Wikimedia-Kontakt. Die Launch-Text-Entwürfe
   sind eine eigene Aufgabe nach 011 (Solo-Founder-Voicing, `wiki/roadmap.md`).
