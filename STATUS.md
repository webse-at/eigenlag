# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 009 — CLI `eigenlag analyze`: der Report ist das Produkt (2026-07-15)

**Das Tool ist jetzt in fremde Hände gebbar.** `pipx install .` → `eigenlag analyze PFAD`
mit `--db`/`--rest`/`--assume-duration` liefert den deutschen Report (Urteil zuerst,
Kreis doppelt, Monte Carlo, What-if-Ranking, Pflicht-Warnblock, Modellgrenzen) und per
`--json` dieselben Felder maschinenlesbar (stabile Keys, ab 010 vom CI-Gate gelesen).
Exit-Codes: 0 analysiert (auch instabil), 1 Bedienfehler, 2 kein analysierbarer DAG.

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 009; Artefakte in `scan/009_cli/`)

| Was | Ergebnis |
|---|---|
| Demo-Pipeline | Alle Prototyp-Pins über die neue Maschinerie: λ 4,40 h, Drift 1,40, What-ifs 3,60 / 2,50 / 3,85 |
| Monte Carlo Perf | **1000 Samples in 0,05 s** (Messvorbehalt Spec 009) → `numpy` bleibt draußen, `dependencies = []` |
| Echtes Airflow | 3.3.0, 12 Läufe, `AIRFLOW_HOME=data/airflow-home/` (liegt noch): CLI-Report aus der echten Metadaten-DB, λ = 1,19 s = mean(arbeit), deckungsgleich 008 |
| Postgres = SQLite | `percentile_cont`-Pfad == Python-Aggregation auf denselben Fixture-Zeilen (Wegwerf-Container `postgres:16`, offener 008-Punkt geschlossen) |
| Flaggschiff | `load_data_wikiviews --assume-duration 300`: λ = 600 s, Kreis `сheck_data → load_data`, voller Report im Log |
| ADR-021 | Selbst-Referenz-Sensor wird Kante; 007-Graph-Check neu: **4836/4836 Karp = Howard**, OmniRoute-Kante modelliert |
| pipx | installiert (apt, 1.8.0), Entry-Point trägt: `eigenlag --help` + `analyze` über die pipx-Installation gelaufen |

### Verifiziert

- `pytest`: **312 passed** (38 neue; jedes neue Modul zuerst rot, Beleg im Log)
- `ruff check`, `ruff format --check` (64 Files), `mypy eigenlag/` (20 Files) grün
- Kern ohne Pflicht-Dependencies; `psycopg2-binary` nur ad hoc in der Dev-venv (Postgres-Beleg), keine Projekt-Dependency

## Hinweise für nächste Session

- **Roadmap: 010 (CI-Gate `eigenlag check --against main`)** ist der nächste Code-Schritt —
  aber die Roadmap setzt den **Feedback-Meilenstein vor den Polish**: das CLI an 2–3 echte
  Teams bringen (Davids Netzwerk, Build-in-public), erst danach über Gate-Umfang, dbt und
  Packaging-Reihenfolge entscheiden (`positioning.md`, Zwischenbewertung). Die Spec für 010
  schreibt der Orchestrator.
- **Für das 010-Gate festlegen:** welcher λ-Wert gegen Schwellen läuft — Punkt-λ auf der
  gewählten Statistik oder MC-λ_p50/λ_p95. Die beiden Schätzer weichen systematisch ab
  (Demo: 4,40 vs. 4,51; Modell-Notiz am Ende von `wiki/log.md`, Session 009).
- **Gleichstand-Befund aus Lauf 4:** bei uniformen Assume-Dauern erreichen oft mehrere
  Kreise dasselbe Zyklusmittel; alle Standard-What-ifs zeigen dann +0 s (korrekt, Report
  benennt es). Fürs Feedback ggf. eine „gemeinsame Engpass-Menge"-Darstellung überlegen —
  erst wenn echte Nutzer darüber stolpern.
- **`data/airflow-home/`** (gitignored) enthält die Airflow-3.3.0-Test-DB der Verifikation;
  wiederverwendbar für 010-Tests, wegwerfbar. `.venv-airflow/` liegt ebenfalls noch.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten.

## Was David entscheiden muss

1. Nichts Blockierendes im Code. Die eigentliche Entscheidung ist der Feedback-Meilenstein:
   an welche 2–3 Teams geht das CLI zuerst?
