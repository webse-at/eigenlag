# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 000 — Orchestrierung (2026-07-13)

Projekt aufgesetzt. **Noch kein Produktiv-Code**, das ist Absicht: Claude ist Orchestrator, Implementierung passiert in eigenen Sessions nach Spec.

### Was liegt

- `CLAUDE.md` — Regeln, Rollen, Pflichtschritte, Anti-Pattern
- `wiki/` — die Wahrheits-Schicht: `index`, `math`, `signals`, `architecture`, `positioning`, `roadmap`, `decisions`, `log`, `changelog`
- `wiki/maxplus_pipeline.py` — der Referenz-Prototyp, unverändert
- `cc-sessions/` — Specs 001 bis 004
- git-Repo initialisiert, noch kein Remote

### Was verifiziert wurde

Der Prototyp wurde ausgeführt. **λ = 4.40 h reproduziert und von Hand hergeleitet**, Drift 1.40 h/Lauf bei T = 3.0 bestätigt. Der ursprünglich offene Blocker (Prototyp nicht auffindbar) ist damit erledigt, ADR-001 aufgelöst. Die Werte sind jetzt Test-Pins für Session 004.

### Nächster Schritt

Zwei Sessions sind sofort startbar und unabhängig voneinander:

- **001** — Scanner-Harvest (Phase 1, GitHub-Kandidaten)
- **004** — Mathe-Kern (Phase 2, am besten abgesichert durch den Prototyp)

Der Auftrag sagt "Phase 1 zuerst komplett". Das bleibt die Vorgabe, solange David nichts anderes sagt.

## Hinweise für nächste Session

### Erledigt seit Session 000

- **Remote:** `https://github.com/webse-at/eigenlag`, `main` gepusht.
- **GitHub-Token geklärt.** Kein neuer Token nötig: `gh` ist als `webse-at` eingeloggt (`repo`, `read:org`, `workflow`). Spec 001 nimmt `GITHUB_TOKEN`, sonst `gh auth token`. Die API ist kostenlos, der Token hebt nur die Limits.

### Offene Entscheidungen

1. **`croniter` als Scanner-Dependency.** Spec 002 erlaubt sie für die Schedule-Klassifikation, aber nur im Scanner, nicht im `eigenlag`-Package. Die Alternative wäre eine eigene Cron-Implementierung, die niemand braucht. Der Implementer entscheidet und begründet im Log.
2. **`pipx` ist nicht installiert.** Wird erst für Session 009 gebraucht, dann mit installieren.
3. **Eigener Scan-Token (optional).** Scanner und Davids normale `gh`-Nutzung teilen sich dieselben 30 Suchen pro Minute. Ein separater fine-grained PAT (nur "Public repositories, read-only") würde das trennen. Komfort, kein Kostenthema.

### Was der Orchestrator prüfen soll

- Nach 001: Ist die Kandidatenmenge groß genug (>= 250 Airflow, >= 120 dbt), **ohne** dass die Filter aufgeweicht wurden? Ein Blick in `rejected.jsonl`, ob die Blocklist zu scharf greift.
- Nach 002: Enthalten die Fixtures wirklich alle Fallen, oder nur die bequemen? Besonders: zwei DAGs in einem File, `depends_on_past=False`, Signal im Kommentar.
- Nach 003: Die Stichprobe ist der eigentliche Prüfpunkt. Zehn Treffer, jeder per Permalink nachgeschlagen. Wenn ein False Positive dabei ist, wurde der Lauf nicht bestanden.
- Nach 004: Stimmen Karp und Howard überein? Ist der Zwei-Perioden-Test wirklich grün, oder wurde `periods` nur im Datentyp geführt und in der Rechnung ignoriert?

### Ungelöste Fragen

- **Wie repräsentativ ist die Scan-Stichprobe?** Öffentliche GitHub-Repos sind nicht Produktions-Pipelines. Der Report muss diese Grenze benennen (Spec 003 erzwingt das), aber die Frage bleibt inhaltlich offen und ist der schwächste Punkt am Marktbeweis.
- **Was, wenn die Risiko-Quote klein ausfällt?** Dann ist die Marktthese schwach, und Phase 2 wäre neu zu bewerten. Das ist ein akzeptables Ergebnis und kein Grund, an den Zahlen zu drehen.
