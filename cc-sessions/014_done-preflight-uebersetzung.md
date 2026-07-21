# Session 014 — Pre-Flight: Übersetzungen und letzte Prüfungen vor dem Public-Schalter

**Basiert auf einem Entwurf von claude.ai (David, 2026-07-21), vom Orchestrator korrigiert.** Drei Änderungen gegenüber dem Entwurf, Begründung in `wiki/log.md` (Eintrag folgt bei Abnahme): (1) `wikimedia/case.md` ist Übersetzungspriorität eins — sie ist das aus Reddit-Post, Wikimedia-Mail, Release-Notes und README verlinkte tragende Dokument; der Entwurf hatte sie übersehen. (2) `CLAUDE.md` wird **nicht** übersetzt — internes Betriebshandbuch, README legt die deutsche Entwicklungs-Doku offen. (3) Aufgabe 3 (positioning) mit korrigierten Optionen: beim Public-Schalter wird die gesamte Git-Historie öffentlich, Verschieben/Kürzen entfernt nichts — die echte Entscheidung ist "dazu stehen oder nicht".

**Bereits erledigt (David, 2026-07-21):** `gitleaks detect` über die volle Historie (52 Commits, 34 MB): no leaks found. `gitleaks dir` über den Arbeitsstand: no leaks found.

## Aufgabe 1 — Übersetzungen (Deutsch → Englisch, Dateien werden ersetzt, Pfade bleiben)

Fachlich präzise, keine Umformulierung der Substanz. Reihenfolge = Priorität:

a) **`wikimedia/case.md`** — die Fallstudie, das tragende öffentliche Dokument. Harte Anforderungen: jede Zahl exakt erhalten, alle PromQL-Blöcke **unverändert** (Code, keine Übersetzung), alle Permalinks unverändert, die Abschnitts-Anker prüfen (falls Launch-Texte oder README auf `#abschnitt` verlinken, müssen die Links nachgezogen oder die Anker erhalten werden). Die ehrlichen Caveats (Rückkopplung, ADR-019-Einordnung "belegt die These, nicht das Werkzeug") wörtlich-sinngemäß erhalten, nicht abschwächen.
b) **`wiki/math.md`** — Formeln, Zahlen, der Prototyp-Pin (λ = 4.40 h, Drift 1.40 h/Lauf bei T = 3.0, kritischer Kreis) exakt erhalten.
c) **`wiki/signals.md`** — die Semantik jeder Signal-Definition darf sich nicht verschieben (insb. `ExternalTaskSensor` nur mit `execution_delta`/`execution_date_fn` als Cross-Run; die λ-Übersetzungstabelle aus ADR-020).
d) **`wiki/index.md`** — oben ein englischer Zweizeiler: Entwicklungsnotizen deutsch (Arbeitssprache), Fallstudie/Mathe-/Signal-Referenz englisch, mit Links.
e) **`CLAUDE.md`** — bleibt deutsch. Nur die Sprachregel-Zeile anpassen: "`wikimedia/case.md`, `wiki/math.md`, `wiki/signals.md` englisch (öffentliche Referenz); übrige `wiki/`-Dateien, `cc-sessions/` und diese Datei deutsch als Entwicklungsarchiv."

**Verifikation der Übersetzungen (Pflicht, nicht Kür):** je Datei alle Zahlen-Tokens vor und nach der Übersetzung maschinell extrahieren (`grep -oE '[0-9][0-9.,]*'` o. ä.) und als Multimengen vergleichen — identisch, sonst ist ein Wert verloren oder verändert. Für `case.md` zusätzlich: alle URLs vorher/nachher identisch. Ergebnis ins Log pasten.

## Aufgabe 2 — Prüfungen (nur Report, nichts ändern)

a) `git log --all --oneline` komplett durchgehen: Commit-Messages, die öffentlich unglücklich wären (Interna, Namen), auflisten. **Nur melden** — Historie bleibt unangetastet, und sie wird mit dem Public-Schalter ohnehin vollständig öffentlich; deutsche Commit-Messages sind kein Problem, konkrete Peinlichkeiten wären eine bewusste Kenntnisnahme-Entscheidung Davids, keine Korrektur.
b) `.claude/`-Ordner sichten: interne Notizen, lokale Pfade, Persönliches auflisten mit Empfehlung (vor Public löschen / unbedenklich). Hinweis: `.claude/` ist untracked (`git status`) — prüfen und im Report bestätigen, dass es in keinem Commit der Historie liegt (`git log --all --oneline -- .claude/` leer).
c) Existenz und Konsistenz aller in `launch/launch-checklist.md` referenzierten Artefakte: `docs/pypi-release.md`, `launch/readme-pypi-install.patch` (`git apply --check`), die vier Launch-Texte, `wikimedia/`, CI-Workflow unter `.github/`, README-Badge-URL, `assets/demo.gif`-Einbettung. Jede fehlende oder inkonsistente Referenz melden.
d) README quer lesen: tote relative Links, deutsche Restfragmente, Versionsangaben. Nach Aufgabe 1 zusätzlich: Links auf jetzt englische Dateien stimmen sprachlich ("development docs are in German" im README-Kopf braucht nach 1d eine Präzisierung).

## Aufgabe 3 — positioning.md: Zitate-Liste für Davids Entscheidung (NICHT entscheiden, NICHT ändern)

`wiki/positioning.md` geht mit dem Public-Schalter online, und zwar **inklusive aller historischen Fassungen in der Git-Historie** — Kürzen oder Verschieben innerhalb des Repos entfernt nichts. Die Datei nicht anfassen. Liefern: die 3–5 Stellen, die ein Reddit-Leser gegen David wenden könnte (Kandidaten: "Verkauft Können statt Angst", die "Kauf-Stärke"-Rangfolge der Trigger-Momente, "Der Post führt mit Trigger 4 … die Kommentare zeigen, welcher Moment zieht", das Abbruchkriterium), wörtlich zitiert, je eine Zeile Einschätzung.

**Empfehlung des Orchestrators, im Report so wiedergeben:** dazu stehen. Das Dokument ist offene Produkt-Selbstprüfung mit Abbruchkriterium — als Fund in einem Build-in-public-Repo ist es verteidigbar, ein nachträglich saniertes Dokument mit Original in der Historie wäre es nicht. Die Entscheidung trifft David als Klartext-Frage am Ende des Reports.

## Aufgabe 4 — launch-checklist.md konsolidieren

a) Schritt 0 einfügen, als erledigt dokumentiert: Secret-Scan der vollen Historie (gitleaks v8.30.1, 52 Commits, no leaks found, 2026-07-21) plus Ergebnis der Reviews aus Aufgabe 2a/2b.
b) In Schritt 1 den Prüf-Halbsatz durch den Verweis auf Schritt 0 ersetzen.
c) Vor Schritt 2 ergänzen: die Übersetzungs-Commits aus Aufgabe 1 triggern den CI-Lauf — dieser Lauf ist der Badge-Beleg.
d) Die positioning-Entscheidung als expliziten offenen Entscheidungspunkt vor Schritt 1 eintragen (Davids Entscheidung, mit Verweis auf den Report aus Aufgabe 3).
e) DRAFT-Markierung entfernen, wenn danach nichts mehr offen ist außer Davids Schaltern und der positioning-Frage.

## Grenzen

- KEIN Checklisten-Schritt wird ausgeführt: kein Visibility-Change, kein PyPI, kein Tag, kein Post, keine Mail.
- Keine Historie umschreiben, kein rebase, kein filter-branch, kein `--force`.
- `scan/` und `wikimedia/`-Zahlen bleiben unangetastet (Übersetzung von `case.md` ändert Sprache, nie Werte).
- `launch/`-Texte werden nicht redigiert (Davids Stimme) — mit einer Ausnahme: wenn Aufgabe 1a Anker/Links bricht, werden exakt diese Links nachgezogen und im Log ausgewiesen.

## Abschluss

Pflichtschritte aus CLAUDE.md (STATUS, Log, ggf. Changelog, Tests/ruff/mypy mit gepastetem Output, Commit und Push). Übersetzungs-Commit(s) getrennt vom Checklisten-Commit; `case.md` als eigener Commit. Am Ende: kompakter Report an David — Ergebnisse aus Aufgabe 2, die Zitate-Liste aus Aufgabe 3 und die offene positioning-Frage als Klartext.
