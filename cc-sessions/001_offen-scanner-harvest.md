# Session 001 — Scanner: Harvest-Schicht

**Phase 1, Schritt 1 von 3.** Ziel dieser Session ist **nur** die Kandidaten-Liste. Kein Clone, keine AST-Analyse, kein Report. Wer hier schon analysiert, macht die Session unprüfbar.

## Vorher lesen

- `CLAUDE.md` (harte Regeln, besonders 6, 7, 8)
- `wiki/index.md`, `wiki/signals.md`, `wiki/architecture.md`

## Auftrag

Baue `scanner/harvest.py`: findet über die GitHub-Code-Search-API Repo-Kandidaten und schreibt sie deduplizert und gefiltert nach `data/candidates.jsonl`.

### 1. Auth

Token aus `GITHUB_TOKEN`. Wenn nicht gesetzt, Fallback auf `gh auth token` per Subprocess. Wenn beides fehlt: sauber abbrechen mit einer Meldung, was zu tun ist. Nicht ohne Token weiterlaufen, die Rate-Limits sind sonst unbrauchbar (10 statt 30 Requests pro Minute).

### 2. Queries

Airflow:
```
depends_on_past language:python
wait_for_downstream language:python
ExternalTaskSensor execution_delta language:python
include_prior_dates language:python
prev_start_date_success language:python
```
dbt:
```
is_incremental language:sql path:models
```

Die Code-Search liefert maximal 1000 Ergebnisse pro Query. Das reicht für die Zielmenge, aber die Beschränkung muss im Report ehrlich benannt werden: die Stichprobe ist nicht repräsentativ für "alle Airflow-Repos der Welt", sondern für "Repos, die diese Begriffe enthalten und über die Code-Search auffindbar sind". Diese Einschränkung gehört als Satz in `report.md` (Session 003).

**Wichtig:** Die Code-Search sucht Volltext, nicht AST. Ein Treffer hier ist ein *Kandidat*, kein Signal. Die Entscheidung, ob wirklich ein Cross-Run-Signal vorliegt, fällt erst in Session 002 per AST. Diese Trennung ist der ganze Punkt der Übung.

### 3. Filter

- Forks raus (`fork: false`)
- Repo-Größe < 150 MB (`size` aus der Repo-API, in KB)
- Blocklist-Regex gegen Repo-Vollname **und** Beschreibung, case-insensitive:
  `awesome|tutorial|course|template|example|demo|playground|learning|training|bootcamp|workshop|starter|boilerplate|cookiecutter|sandbox|test-repo`
- Archivierte Repos raus
- Dedup über `full_name`

Jeder Filter-Grund wird **mitgeschrieben**, nicht stillschweigend angewandt. Eine Zeile pro verworfenem Repo in `data/rejected.jsonl` mit Feld `reason`. Grund: Wenn der Blocklist-Regex am Ende 40 Prozent der Kandidaten frisst, muss David das sehen und die Liste anfechten können. Ein stiller Filter ist eine unbelegte Behauptung.

### 4. Rate-Limits und Fehler

- Code-Search mit Token: 30 Requests pro Minute. Halte dich daran, proaktiv drosseln, nicht ins 403 laufen.
- Bei `403` mit `x-ratelimit-remaining: 0`: bis `x-ratelimit-reset` schlafen, dann weiter.
- Bei `secondary rate limit`: exponentieller Backoff, Start 2 s, Faktor 2, Deckel 60 s, maximal 5 Versuche.
- Jeder Fehler strukturiert nach `data/scan_errors.jsonl`: Zeitstempel, Query oder Repo, HTTP-Status, Message. Der Lauf bricht **nicht** ab.

### 5. Resume

`candidates.jsonl` und `rejected.jsonl` werden append-only geschrieben. Vor Start wird gelesen, was schon drin ist, und bereits abgearbeitete Queries plus Seiten werden übersprungen. Ein Fortschritts-File `data/harvest_state.json` hält fest, welche Query bei welcher Seite steht. Ein Abbruch nach 800 von 1000 Ergebnissen darf nicht bedeuten, dass alles neu läuft.

## Ausgabe-Format

`data/candidates.jsonl`, eine Zeile pro Repo:

```json
{"full_name": "org/repo", "html_url": "...", "default_branch": "main",
 "size_kb": 4210, "stars": 128, "pushed_at": "2026-03-11T...",
 "matched_queries": ["depends_on_past language:python"],
 "matched_paths": ["dags/etl.py", "dags/ml.py"]}
```

`matched_paths` ist wertvoll: Session 002 kann damit gezielt die Files anschauen, statt das ganze Repo zu durchsuchen. Aber sie ist **nicht** die endgültige File-Liste, weil ein DAG-File mit einem Signal in `default_args` von der Volltext-Suche unter Umständen nicht gefunden wird. Session 002 scannt trotzdem alle Python-Files unter den üblichen Pfaden.

## Akzeptanz

- `python -m scanner.harvest` läuft durch und schreibt `candidates.jsonl`
- Mindestens 250 Airflow-Kandidaten und 120 dbt-Kandidaten nach Filterung. Falls weniger: **nicht** die Filter aufweichen, sondern melden und mit David besprechen. Eine kleinere ehrliche Menge ist besser als eine aufgeblähte.
- Ein absichtlicher Abbruch (Ctrl-C) und Neustart setzt fort, statt neu zu beginnen. Beleg: Zeilenzahl vor und nach dem Neustart im Session-Log.
- `pytest` grün für die Filter-Logik (Blocklist, Größe, Dedup) mit Tabellen-Tests. Die HTTP-Schicht wird nicht gemockt-getestet, sondern durch den echten Lauf belegt.
- `ruff check .` und `ruff format --check .` grün

## Explizit nicht in dieser Session

Clonen, AST, CSV, `report.md`. Das ist 002 und 003.

## Pflichtschritte am Ende

Siehe `CLAUDE.md`, Abschnitt "Pflichtschritte Ende jeder Implementer-Session". Insbesondere: echten Lauf-Output ins Session-Log pasten, nicht behaupten, dass es läuft.
