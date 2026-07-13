# Roadmap

Der Session-Plan. Jede Zeile ist genau eine CC-Session mit genau einer Spec in `cc-sessions/`.

## Phase 1 — Scanner v2 (Marktbeweis, Launch-Content)

| Session | Inhalt | Abhängig von |
|---|---|---|
| 001 | Harvest: GitHub Code-Search, Repo-Kandidaten, Dedup, Fork- und Blocklist-Filter, Größenlimit. Ergebnis: `candidates.jsonl` | keine |
| 002 | AST-Analyse: DAG-Erkennung, Signale A-F DAG-scoped, Schedule-Klassifikation, dbt-Scan. Ergebnis: getestete Analyse-Funktionen | 001 nicht zwingend, Fixtures reichen |
| 003 | Scan-Lauf über >= 200 Airflow- und >= 100 dbt-Repos, `scan_results.csv`, `report.md`, Stichprobe von 10 Treffern verifiziert | 001, 002 |

**Akzeptanz Phase 1:** Lauf ohne Absturz, CSV und Report liegen vor, zehn Treffer wurden per Repo, Datei und Zeile auf GitHub nachgeschlagen und bestätigt.

## Phase 2 — Analyzer-Core als CLI `eigenlag`

| Session | Inhalt | Abhängig von |
|---|---|---|
| 004 | Mathe-Kern: `model.py`, `maxplus.py` (Kondensation, Karp, Howard, Drift). Tests gegen die Prototyp-Werte. Perioden-Versatz in der Cross-Kante | keine, Prototyp liegt vor |
| 005 | Airflow-Parser per AST: Tasks, `>>`/`<<`, `set_upstream/downstream`, Signale, Schedule | 004 |
| 006 | dbt-Parser (`manifest.json`) plus Dauern-Schicht (Metadaten-DB, REST, `--assume-duration`) | 004 |
| 007 | CLI `eigenlag analyze`, deutscher Report, What-if-Ranking, Monte Carlo (λ_p50, λ_p95) | 005, 006 |
| 008 | CI-Gate `eigenlag check --against main`, Exit-Code, fertiger PR-Kommentar-Text | 007 |
| 009 | Packaging, `pipx install .` verifiziert, README mit dem Bäckerei-Beispiel | 008 |

**Akzeptanz Phase 2:** `pipx install .` läuft, `eigenlag analyze <pfad> --db <url>` liefert λ, kritischen Kreis und What-if-Ranking, CI-Gate-Test grün.

## Reihenfolge

Phase 1 zuerst komplett, dann Phase 2. Grund: Die Marktzahlen entscheiden, ob das Produkt überhaupt gebaut werden soll. Ein Scan, der zeigt, dass Cross-Run-Kanten praktisch nie auf sub-tägliche Schedules treffen, würde Phase 2 erübrigen. Diese Möglichkeit ist real und wird nicht wegdefiniert.

Session 004 hängt nicht am Scanner und könnte parallel laufen, wenn David das will. Sie ist die inhaltlich interessanteste und die am besten abgesicherte, weil der Prototyp als Referenz vorliegt.

## Bewusst nicht gebaut

- **Web-UI oder Dashboard.** Das Tool ist ein CLI und ein CI-Gate. Ein Dashboard ist ein anderes Produkt.
- **Live-Monitoring.** Kein Daemon, kein Agent im Cluster. Der Analyzer läuft auf Anforderung.
- **Scheduler-Optimierung.** Das Tool sagt, wo der Engpass ist. Es räumt ihn nicht weg.
- **Dagster- und Prefect-Parser.** Erst wenn Airflow und dbt stehen und jemand danach fragt.
