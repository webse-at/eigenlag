# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 008 — Dauern-Schicht: aus Struktur-Aussagen werden Zeit-Aussagen (2026-07-15)

**Das erste λ in Sekunden steht, end-to-end.** `eigenlag/durations.py` beschafft echte
Task-Dauern (Metadaten-DB via sqlalchemy als Extra `[db]`, REST via urllib, `assume` als
Fallback; je Task p50/p95/mean/n/operator/is_sensor), `eigenlag/analyze.py` komponiert
parsen → Dauern heiraten → kondensieren → Howard. Sensor auf dem kritischen Kreis erzeugt
die Pflicht-Warnung (`sensor_im_kritischen_kreis`), Mischbetrieb und Mindest-Stichprobe
(n < 5) warnen je Task und fallen auf den Assume-Wert.

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 008)

| Was | Ergebnis |
|---|---|
| Schema-Verifikation | Airflow **3.3.0** standalone (`.venv-airflow`, Python 3.12.13): alle `task_instance`-Annahmen halten (Spalten, Sekunden, TaskGroup-Prefix `grp.laden`, `operator`-Klassenname) |
| REST-Befund | **Airflow 3 hat `/api/v1` (404) und Basic Auth (401) entfernt** → `from_rest(..., api_version="v2")` mit JWT-Token als Default, `"v1"`+Basic für Airflow 2. DB- und REST-Pfad liefern identische Statistik auf denselben Läufen |
| End-to-End echt | `analyze(testfall_dop_sensor)` mit DB-Dauern: **λ = 1,186 s** (Selbstkante `arbeit`), CP 2,977 s — Teilpfad-Fall in Sekunden |
| End-to-End Flaggschiff | `load_data_wikiviews` mit `assume(300)`: **λ = 600 s** vs. CP 900 s = die 007-Struktur (2 vs. 3) × 300; 8 `dauer_angenommen`-Warnungen |
| Sensor-Nachlauf (14 Fälle, ganze Repos) | **1 modellierbar / 11 weiterhin nicht / 2 Ziel nicht im Repo** (`scan/008_sensor/nachlauf.csv`, je mit Permalink). Kein `periods > 1` in freier Wildbahn — ADR-006 bleibt test-belegt |

### Verifiziert

- `pytest`: **274 passed** (18 neue; beide neuen Module zuerst rot — `ModuleNotFoundError` im Log belegt)
- `ruff check`, `ruff format --check` (38 Files), `mypy` (38 Files) grün
- Kern ohne Pflicht-Dependencies (`dependencies = []`); `sqlalchemy` nur Extra `db` + `dev`

## Hinweise für nächste Session

- **Roadmap: 009 (CLI, Report, Monte Carlo, What-if)** ist der nächste Schritt. `analyze()`
  liefert bereits alles, was der Report braucht (λ, Kreis kondensiert + aufgelöst, CP,
  drei Warnungs-Sorten). Der Report soll λ auf `mean` **und** `p95` nebeneinander zeigen
  (Vorentscheid 3 der Spec 008) und die F-Divergenz erklären (ADR-020).
- **dbt-Parser ist offen:** er stand ursprünglich in 008, wurde beim Spec-Schnitt
  herausgenommen (Roadmap-Zeile 008b, Spec fehlt). Vor oder mit 009 entscheiden, ob die
  CLI Airflow-only startet.
- **Für den Orchestrator zu prüfen:** (1) **Selbst-Referenz-Sensor als ADR-Kandidat** —
  `bhatiadeepak0805/OmniRoute_Project_Group_4`, `DAG_Codes/dag_2.py:480`: ExternalTaskSensor
  auf den *eigenen* DAG (5-h-Versatz). Der Parser modelliert nur Fremd-DAG-Sensoren; semantisch
  wäre das eine Selbst-Rekurrenz-Kante. Lohnt eine Regel, oder bleibt es Warnung? (2) Der
  PostgreSQL-Pfad von `from_metadata_db` (percentile_cont-SQL) ist gegen kein echtes Postgres
  gelaufen — nur SQLite war im Standalone-Setup. Die Python-Aggregation ist gegen dieselben
  Pins getestet; ein Postgres-Integrationslauf wäre der fehlende Beleg (z. B. Docker-Postgres
  in 009/010 oder am echten Airflow eines Kunden).
- **Airflow-Verifikations-Setup ist wegwerfbar:** `.venv-airflow/` (gitignored) mit
  AIRFLOW_HOME darin; die beiden Test-DAGs liegen als Beleg in `scan/008_sensor/testfall_*.py`.
  Wiederholung: uv venv, `apache-airflow==3.3.0` + Constraints, `airflow dags test`.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten.

## Was David entscheiden muss

1. Nichts Blockierendes. Session 009 kann starten; die dbt-Frage (Airflow-only-CLI zuerst?)
   entscheidet der Orchestrator beim Spec-Schnitt.
