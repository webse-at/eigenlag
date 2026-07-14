# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 001 — Scanner-Harvest (2026-07-14)

**Die Kandidatenliste steht.** 1692 Repos, belegt und gefiltert, mit protokolliertem Grund für jedes verworfene Repo. Der Mathe-Kern aus Session 004 ist unangetastet.

### Was liegt

- `scanner/harvest.py` — sechs Code-Search-Queries gegen `/search/code`, Drosselung beider Kontingente (`search` 30/min, `core` 5000/h), Filter, Resume.
- `scanner/harvest_test.py` — 26 Tests: Filter-Tabelle, Dedup, Seiten-Fortschaltung, Query-Liste.
- `data/` (nicht im Repo, per `.gitignore`): `candidates.jsonl` (1692), `rejected.jsonl` (403), `hits.jsonl` (5520 Rohtreffer), `harvest_state.json`, `scan_errors.jsonl`.
- `pyproject.toml` — `mypy` prüft jetzt `eigenlag` **und** `scanner`.

### Was verifiziert wurde

- **Echter Lauf:** 2095 Repos bewertet, **1692 Kandidaten** (1328 Airflow, 364 dbt), 403 verworfen (251 Blocklist, 152 Größe). Akzeptanzschwelle der Spec (250 / 120) deutlich übertroffen, ohne einen Filter aufzuweichen.
- **Resume:** Lauf nach der ersten Query abgebrochen (1000 Zeilen in `hits.jsonl`), Neustart meldet `[fertig] depends_on_past ... (aus vorigem Lauf)` und zieht die 1000 Treffer nicht erneut.
- **Drosselung:** `search`-Kontingent dreimal erschöpft, jedes Mal bis zum Reset gewartet und weitergelaufen. Ein 502 strukturiert in `scan_errors.jsonl`, vom Folgelauf automatisch nachgeholt.
- **Stichprobe:** 10 Kandidaten zufällig gezogen, alle auf Datei und Zeile aufgelöst. Belege im Log, Eintrag 001.
- `pytest` 62 passed, `ruff check`, `ruff format --check`, `mypy` grün.

### Nächster Schritt

**002 — Scanner: AST-Analyse.** Die Spec liegt unter `cc-sessions/002_offen-scanner-ast.md`. Sie kann direkt auf `data/candidates.jsonl` aufsetzen.

## Hinweise für nächste Session

### Neue Entscheidung

- **ADR-008** — Der Harvest ist zweistufig. Stufe 1 schreibt Rohtreffer nach `hits.jsonl`, Stufe 2 holt Metadaten und filtert. Die Resume-Grenze liegt zwischen beiden, damit ein Abbruch das knappe `search`-Kontingent schützt und nicht das reichliche `core`-Kontingent.

### Was der Orchestrator prüfen soll

1. **Die Stichprobe ist der eigentliche Befund der Session.** Von zehn zufälligen Kandidaten sind fünf inhaltlich **kein** Signal: dreimal `'depends_on_past': False`, einmal auskommentiert, einmal die Signatur einer eigenen Operator-Klasse. Die Code-Search-Zahl taugt damit unter keinen Umständen als Marktzahl. Für `report.md` (Session 003) heißt das: die Risiko-Quote wird ausschließlich auf die AST-Ergebnisse aus 002 bezogen, und der Nenner ist zu benennen.
2. **Vier der sechs Queries laufen in den 1000er-Deckel** der Code-Search (`depends_on_past` meldet `total_count` 2284, geholt: 1000). Die Stichprobe ist nach oben abgeschnitten und nicht repräsentativ für "alle Airflow-Repos". Der Einschränkungssatz aus Spec 001 muss in `report.md` wirklich stehen.
3. **`fork` und `archived` haben null Mal gegriffen.** Nachgeprüft an 20 zufälligen Kandidaten: alle `fork=false archived=false`. Die klassische Code-Search liefert offenbar weder Forks noch archivierte Repos. Kein Filter-Fehler, aber im Report zu erwähnen, sonst liest es sich wie einer.
4. **Blocklist frisst 12 Prozent** (251 von 2095). Anfechtbar über `rejected.jsonl`, jede Zeile mit `reason` und `description`.

### Offene Entscheidungen

1. **`croniter` als Scanner-Dependency** (aus Session 000 offen, in 002 zu entscheiden). Spec 002 erlaubt sie im Scanner, nicht im `eigenlag`-Package.
2. **`pipx` ist nicht installiert.** Wird für Session 009 gebraucht.
3. **`numpy` wird im Kern nicht gebraucht**, erst bei Monte Carlo (Session 006).

### Ungelöste Fragen

- Unverändert: was passiert, wenn die Risiko-Quote nach der AST-Analyse klein ausfällt. Session 002 liefert die Zahl, die das entscheidet.
