# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 003 — Scanner: Lauf, CSV, Report (2026-07-14)

**Phase 1 ist technisch abgeschlossen und inhaltlich negativ ausgegangen.** Der Scanner läuft
über die volle Kandidatenliste, jede Zahl hat einen Nenner, jeder Treffer einen Permalink auf
den Commit-SHA. Der Marktbeweis, den Phase 1 erbringen sollte, ist damit **nicht** erbracht.

### Was liegt

- `scanner/run.py` — Lauf über `data/candidates.jsonl`, resume-fähig (State-File je Repo unter
  `data/scan_state/`), Clone-SHA für die Permalinks, Fehler nach `data/scan_errors.jsonl`.
- `scanner/report.py` — `scan/scan_results.csv` (eine Zeile je DAG), `scan/scan_factories.csv`,
  `scan/scan_dbt.csv`, `scan/report.md`. Airflow und dbt mit getrennten Nennern (ADR-012).
- `scan/sample_verification.md` — beide Stichproben, je 10 Zeilen, mit Urteil.
- `scanner/analyze.py` — zusätzlich `task_count` je DAG, Signal F an drei Fundorten (ADR-013),
  `execution_delta=0` ist kein Signal (ADR-014).

### Was verifiziert wurde

- `pytest`: **140 passed**, `ruff check`, `ruff format --check`, `mypy` grün.
- **Voller Lauf (Lauf 3, final):** 1692 Repos, 1671 geklont, 21 Clone-Fehler, 317.706 Files
  geparst, 7590 `SyntaxError` protokolliert, kein Abbruch. **51.426 DAGs, 1303 mit Cross-Run
  (2,5 %), 2543 sub-täglich (4,9 %), 176 Risiko-Kandidaten (0,3 %) in 100 Repos.** dbt: 496
  Repos mit `dbt_project.yml`, 37.109 Models, 3369 mit echter Selbst-Kante (9,1 %).
- **Stichprobe 8a:** 10 Risiko-Kandidaten gegen den Quelltext geprüft, **0 Falsch-Positive**.
- **Stichprobe 8b:** 10 signalfreie Kandidaten-Repos geprüft, **0 unbekannte Muster**.

### Der Befund, an dem alles hängt

**138 der 176 Risiko-Kandidaten (78 %) sind Beispiel-, Test- oder Doku-Code**, ebenso 75 % aller
gefundenen DAGs. Airflows eigener Demo-DAG `example_branch_dop_operator_v3` trägt
`depends_on_past=True` bei `*/1 * * * *` und wird massenhaft geforkt. Die Risiko-Quote misst
damit vor allem die Kopierfreude von Lernenden. Öffentliche Repos sind für diese Frage der
falsche Beweisort: dort liegt Lernmaterial, und Laufzeiten liegen dort ohnehin nicht.

Die Definition wurde **nicht** gelockert (ADR-005 und Spec 003, Abschnitt 7). Die Aussage, die
hält, steht im Report: 1303 Airflow-DAGs und 3369 dbt-Models haben einen Kreis über die
Zeitachse, und für keinen einzigen ist bekannt, wo seine Taktgrenze liegt.

## Hinweise für nächste Session

### Neue Entscheidungen

- **ADR-013** — Signal F wird auch als Parametername einer Callable und als Template in einer
  Modul-Variablen erkannt. Gefunden über die Negativ-Suche, nicht über die Stichprobe.
- **ADR-014** — `execution_delta=timedelta(hours=0)` ist kein Cross-Run-Signal. Gefunden als
  Falsch-Positiv in der Stichprobe, Ursache behoben, Lauf wiederholt.

### Nächster Schritt: Session 005 — der Wikimedia-Fall

Spec liegt unter `cc-sessions/005_offen-wikimedia-case.md`. Die Recherche nach einem Beweisort
mit echten Laufzeiten ist gelaufen (`wiki/log.md`, Eintrag 003a): dbt-Artefakte auf GitHub sind
Compile-Läufe ohne Ausführung, GitHub-Issues und Stack Overflow sind dünn, **Wikimedia trägt**.
Neun Airflow-Instanzen in Produktion, Grafana anonym abfragbar, DAG-Code offen auf GitLab.

Der Fall: `search/dags/rdf_streaming_updater_reconcile.py:110`, stündlicher Takt,
`depends_on_past: True`, `max_active_runs=1`, gemessene mittlere Laufdauer 60 bis 109 Minuten.
Takt kürzer als die Laufzeit, Kreis über die Zeitachse. Zu belegen ist die Gauge-Semantik der
Metrik, bevor eine Zahl behauptet wird.

**Zweiter Befund, der in 005 mitläuft:** Der Scanner sieht in Wikimedias Repo 71 von 325 DAGs
und null Signale, weil dort DAGs über die eigene Wrapper-Funktion `create_easy_dag()` entstehen.
Professionelle Umgebungen kapseln, und genau die sind für uns blind. Vorschlag ADR-015, die
Entscheidung gehört in den Plan von 005.

### Was David und der Orchestrator entscheiden müssen

1. **Phase 2 startet nicht blind.** Der Marktbeweis über öffentlichen Code ist gescheitert, und
   zwar nicht am Scanner, sondern an der Grundgesamtheit. Vor weiterem Produktbau braucht es
   einen Beleg aus einer Quelle, die Laufzeiten kennt.
2. **Der Beweisort ist gefunden und geprüft: Wikimedia.** Die beiden anderen Spuren sind tot
   (siehe `wiki/log.md`, 003a). Ein Tauschangebot in Communities scheidet auf Davids Wunsch aus.
3. **Der Scan taugt als Build-in-Public-Content**, gerade weil die Zahl klein ist. Nicht für die
   Agentur-Kanäle.

### Offene Punkte aus früheren Sessions

- `pipx` ist nicht installiert, wird für Session 009 gebraucht.
- `numpy` wird im Kern erst bei Monte Carlo (Session 006) gebraucht.
- Der Clone-Cache liegt mit 74 GB unter `data/repos/`, die Stände der Läufe 1 und 2 unter
  `data/scan_state_run1` und `_run2`. Nichts davon ist getrackt, nichts wurde überschrieben.
