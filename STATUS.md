# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 014 — Pre-Flight vor dem Public-Schalter (2026-07-21)

**Alles für Davids Schalter liegt bereit; veröffentlicht hat die Session nichts.**
Die öffentliche Referenz ist jetzt englisch, die Launch-Checkliste ist konsolidiert und
nicht mehr DRAFT. Offen sind nur noch Davids manuelle Schalter (`launch/launch-checklist.md`)
und eine Klartext-Entscheidung zu `wiki/positioning.md` (siehe unten).

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 014)

| Was | Ergebnis |
|---|---|
| Übersetzung DE→EN | `wikimedia/case.md`, `wiki/math.md`, `wiki/signals.md` englisch. Werte, Permalinks, Code-Blöcke unverändert; Zahlen locale-korrekt (Dezimalpunkt, Tausender-Komma) |
| Zahlen-Verifikation | locale-bewusster Multimengen-Diff DE(HEAD) vs. EN: case 119 Tokens / 71 Werte, math 34 / 19, signals 38 / 24 — je identisch. URLs (case: 8) und Code-Blöcke identisch |
| index/CLAUDE/README | index.md engl. Zweizeiler oben; CLAUDE.md Sprachregel präzisiert; README-Kopfzeile "development docs are in German" wahrheitswahrend nachgezogen (Folge aus der Übersetzung) |
| Checkliste | Schritt 0 (Sicherheits-/Historien-Review, erledigt) + positioning-Entscheidungspunkt + CI-Badge-Auslöser ergänzt, DRAFT entfernt |
| Prüfungen | Commit-Historie, `.claude/`, Checklisten-Artefakte, README — alle sauber/konsistent (Details unten) |

### Verifiziert

- `pytest`: **377 passed**. `ruff check`, `ruff format --check` (53 Files), `mypy` (29 Files)
  grün. Unverändert gegenüber 013, da diese Session nur Markdown berührt hat.
- Übersetzungs-Diff mit `scratchpad/numcheck.py` (locale-bewusst); Ausgaben im Log.
- `readme-pypi-install.patch`: `git apply --check` sauber. Badge-URL = Workflow-Pfad.
  Alle relativen README-Links auflösen. Keine Anker-Links auf `#abschnitt` der drei Dateien.

## Hinweise für nächste Session

- **Abnahme 014 durch den Orchestrator steht aus.** Prüfschwerpunkte: die englische Fassung
  von `case.md` gegen das deutsche Original auf fachliche Treue (nicht nur Zahlen — die
  ehrlichen Caveats zur Rückkopplung und die ADR-019-Einordnung "belegt die These, nicht das
  Werkzeug" müssen sinngleich stehen); die README-Kopfzeilen-Korrektur; die Checklisten-
  Konsolidierung.
- **`.claude/settings.json` ist untracked und nicht ignoriert.** Enthält lokale Pfade + einen
  jukeep.com-Verweis. Empfehlung an David: `.claude/` in `.gitignore` aufnehmen, damit ein
  versehentliches `git add -A` es nicht öffentlich macht. (Über den Visibility-Schalter allein
  wird es nicht öffentlich, solange nicht committet.)
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren mit
  Literal-Argumenten. **dbt-Parser** bleibt bis nach dem Feedback-Meilenstein vertagt.
- **Erster echter CI-Lauf** passiert erst nach dem Public-Schalter (Checklisten-Schritt 2); die
  Übersetzungs-Commits dieser Session sind der erste Push danach und triggern ihn. Das
  3.12-Bein war lokal nicht prüfbar (Server hat nur Python 3.14.4).

## Was David entscheiden muss

1. **positioning.md — dazu stehen oder nicht.** Das Dokument geht mit dem Public-Schalter
   inklusive Git-Historie online; Kürzen/Verschieben im Arbeitsstand entfernt nichts. Die 3–5
   zitierbaren Stellen, die ein Reddit-Leser gegen David wenden könnte, und die Orchestrator-
   Empfehlung (dazu stehen: offene Produkt-Selbstprüfung mit Abbruchkriterium ist in einem
   Build-in-public-Repo verteidigbar) stehen im Session-014-Report. Als Entscheidungspunkt vor
   Schritt 1 der Checkliste eingetragen.
2. **Die Schalter**: Repo public → CI prüfen → PyPI → README-Patch → Tag v0.1.0 → About/Topics
   → Pause → Wikimedia-Mail → Reddit → Slack → Woche-1-Reaktionsdienst. Reihenfolge, Wirkung und
   Risiko je Schritt in `launch/launch-checklist.md`; nichts davon löst eine Session aus.
3. Die fünf Launch-Texte redigieren — bewusst "fertig zum Redigieren", nicht "fertig zum Posten".
