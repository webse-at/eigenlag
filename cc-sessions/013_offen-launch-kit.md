# Session 013 — Launch-Kit: der 60-Sekunden-Einstieg und die Texte

**Die letzte Session vor Davids Schaltern.** Danach liegt alles bereit: der Sofort-Einstieg (`eigenlag demo`), das Demo-GIF, sichtbare CI, PyPI-fertige Metadaten, und die Launch-Texte als redigierbare Entwürfe. **Diese Session veröffentlicht nichts**: kein PyPI-Upload, kein Repo-Sichtbarkeits-Wechsel, kein Post, keine Mail. Alles, was nach außen geht, löst David aus — die Session baut ausschließlich Material.

## Vorher lesen

- `wiki/positioning.md` vollständig (Trigger-Momente, Produkt-Ebene, Abbruchkriterium) — die Launch-Texte erzählen genau das
- `wiki/roadmap.md`, Abschnitt Feedback-Meilenstein (die Schalter-Reihenfolge)
- **Davids Schreibstil-Regeln aus der globalen CLAUDE.md — für die Launch-Texte sind sie das wichtigste Dokument dieser Session.** Sinngemäß auf Englisch: ruhige ganze Sätze, keine Antithesen-Stakkatos ("X, not Y"), keine lapidaren Pointen, kein Selbstlob, keine Phrasen, die nach KI klingen, keine Gedankenstrich-Ketten. Die zwei Tests gelten: Würde ein kompetenter Mensch das so im Gespräch sagen? Merkt man, dass es von KI kommt?
- Abnahme 012a (der "foreign task"-Feinschliff gehört hierher)

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **`eigenlag demo` ist ein eingebauter Subcommand**, kein Beispiel-File zum Herunterladen. Er rendert den vollen Report der Demo-Pipeline (die 8-Task-ML-Pipeline aus dem Prototyp, Dauern in Stunden, T = 3 h) über denselben `compose()`/`render()`-Pfad wie `analyze`, in beiden Sprachen. Kopfzeile sagt ehrlich, dass es ein eingebautes Beispiel ist, Fußzeile zeigt den nächsten Schritt (`eigenlag analyze your/dags --assume-duration 300`). Kein Netz, keine Dateien, unter einer Sekunde. Das ist der Aha-Moment für jemanden, der abends am Laptop keine Firmen-DAGs zur Hand hat.
2. **Das "Video" ist ein Terminal-GIF, deterministisch aus einem Skript gebaut, kein manuell aufgenommener Screencast.** Werkzeug: `vhs` (charmbracelet) mit einer `.tape`-Datei im Repo — damit ist das GIF reproduzierbar, wenn sich der Report ändert, und niemand muss je wieder "aufnehmen". Inhalt, bewusst kurz (≤ 30 s Laufzeit, Ziel < 3 MB): `pipx install eigenlag` (eine Zeile, darf gestellt sein) → `eigenlag demo` → der Report scrollt, hält auf der Plan-Sektion (der −43-%-Moment). GIF nach `assets/demo.gif`, im README ganz oben eingebettet. `vhs` wird lokal installiert (Go-Binary bzw. Paket), ist Dev-Werkzeug, keine Dependency.
3. **CI-Workflow:** `.github/workflows/ci.yml`, Matrix Python 3.12 und 3.14, Schritte pytest/ruff/mypy, Badge ins README. **Wichtig:** Vorher prüfen, welche Tests lokale Ressourcen brauchen (Docker/Postgres, Airflow-venv, `data/`-Clones, Netz) und sie CI-fest machen (Skip-Marker mit Begründung, nicht löschen). Die Suite muss auf einem nackten GitHub-Runner grün sein — das lässt sich lokal nicht perfekt beweisen, also so nah wie möglich: frischer Clone in Temp-Verzeichnis, frische venv, `pip install -e .[db] pytest ruff mypy`, Suite grün, und der Workflow-YAML wird gegen genau diese Schrittfolge geschrieben.
4. **PyPI: vorbereiten, nicht hochladen.** Name `eigenlag` ist frei (geprüft 2026-07-16, HTTP 404 auf `pypi.org/pypi/eigenlag/json`). Die Session prüft `python -m build` erneut, validiert die Metadaten (`twine check dist/*` — twine als Dev-Werkzeug ist ok), und legt eine Schritt-für-Schritt-Anleitung für David ab (Account, Token, `twine upload`, danach README-Install-Zeile von `git+https` auf `pipx install eigenlag` umstellen — diese Umstellung als vorbereiteter, aber **nicht gemergter** Patch oder klar markierter Folgeschritt, denn vor dem Upload wäre die Zeile eine Lüge).
5. **Launch-Texte als Dateien unter `launch/`**, alle englisch, alle als Entwurf markiert: `reddit-post.md`, `wikimedia-mail.md`, `airflow-slack.md`, `release-notes-v0.1.0.md`, `launch-checklist.md`. David redigiert und versendet. Nichts davon ist "fertig zum Posten", alles ist "fertig zum Redigieren".

## Auftrag

### 1. `eigenlag demo`

Wie Vorentscheidung 1. Tests: Subcommand existiert, EN und DE, Output enthält die Plan-Sektion mit dem Tragfähigkeits-Marker, Exit 0, und die Kopfzeile weist das Beispiel als Beispiel aus. Dazu der 012a-Feinschliff: den "foreign task"-Katalogsatz umformulieren (Richtung: "the plan shows the arithmetic; whether and how to split the task is yours to judge"), EN und DE, Katalog-Test bleibt grün.

### 2. GIF

`launch/demo.tape` (vhs-Skript), `assets/demo.gif`, README-Einbettung oben (nach dem Einzeiler, vor dem Sauerteig). Größe im Log belegen. Wenn `vhs` auf dem Server nicht installierbar ist (kein Go, kein Paket): asciinema + `agg` als Fallback, gleiche Anforderungen — im Log begründen, was genommen wurde.

### 3. CI

Wie Vorentscheidung 3. Der Workflow läuft erst, wenn das Repo public ist und jemand pusht — die Session kann ihn nicht "grün sehen". Deshalb ist der Beleg hier die dokumentierte Frisch-Clone-Probe plus ein YAML, das exakt deren Schritte nachbildet. Ehrlich ins Log schreiben, dass der erste echte CI-Lauf nach dem Public-Schalter zu prüfen ist (steht auch in der Checkliste).

### 4. Die Launch-Texte

**`reddit-post.md`** (r/dataengineering). Führt mit dem Befund, nicht mit dem Tool:

- Titel-Vorschlag in Richtung: "We measured 30 production Airflow DAGs whose median runtime exceeds their schedule interval. 29 of them are fine. Here's what actually decides it."
- Aufbau: der Sweep-Befund mit den Wikimedia-Zahlen (öffentliche Daten, Permalinks) → warum "runtime > schedule" als Diagnose nichts taugt (Überlappung) → was stattdessen entscheidet (die Kante über die Zeitachse, der Sauerteig in zwei Sätzen) → die 48-min-Konstanz als Preis des eingeschwungenen Zustands → am Ende, kurz und offen deklariert: "I built a small CLI that computes this bound from your DAG files; it's open source, feedback welcome" mit Link. Selbst-Autorenschaft offenlegen (Subreddit-Regeln), keine Superlative, keine Emojis, kein "game-changer"-Vokabular.
- Länge: ein Reddit-Post, den man in zwei Minuten liest. Die Kommentar-Strategie (welcher Trigger-Moment zieht) steht in positioning.md und gehört NICHT in den Post.

**`wikimedia-mail.md`**: kurz (unter 200 Wörter), respektvoll, an das Data-Platform-Team (Kanal-Recherche: Phabricator oder öffentliche Mailing-Liste, beides im Entwurf nennen). Inhalt: wir haben eure öffentlichen Airflow-Metriken analysiert, hier ist die Fallstudie mit jeder Zahl belegt, zwei Befunde, die euch interessieren könnten (der wdqs-Gleichgewichtszustand, die 29/30-Fehlalarm-Quote), Korrekturen willkommen, Heads-up bevor wir öffentlich darüber schreiben. Kein Pitch, kein Verkauf.

**`airflow-slack.md`**: drei, vier Sätze für einen passenden Kanal, Kurzfassung des Reddit-Posts mit Link.

**`release-notes-v0.1.0.md`**: was das Tool kann (analyze, check, demo), was es bewusst nicht tut (Limitations in drei Zeilen), Dank an niemanden (es gibt niemanden), Link auf die Fallstudie.

**`launch-checklist.md`** — Davids Schalter-Reihenfolge als abhakbare Liste: Repo public → erster CI-Lauf prüfen (grün? Badge?) → PyPI-Upload (Anleitung) → README-Install-Zeile umstellen → Release v0.1.0 taggen → About/Topics setzen (konkrete Vorschläge: Beschreibungszeile, Topics `airflow`, `data-engineering`, `scheduling`, `pipeline`, `max-plus`) → ein paar Tage Ruhe → Wikimedia-Mail → Reddit-Post → Airflow-Slack → Woche 1: Issues/Kommentare schnell beantworten (David leitet weiter, Orchestrator bereitet Antworten vor). Je Schritt eine Zeile, was er bewirkt und was schiefgehen kann.

### 5. Verifikation

1. `eigenlag demo` über den Entry-Point, EN und DE, Output ins Log.
2. Das GIF existiert, Größe belegt, README rendert es (lokaler Markdown-Check reicht).
3. Frisch-Clone-Probe: Temp-Clone, frische venv, Suite grün, Schrittfolge = Workflow-YAML.
4. `twine check dist/*` bestanden.
5. Alle fünf Launch-Texte liegen unter `launch/`, jeder mit "DRAFT — David redigiert"-Kopfzeile.

## Akzeptanz

- `eigenlag demo` läuft in beiden Sprachen, Test-gepinnt, unter 1 s
- `assets/demo.gif` < 3 MB, aus `launch/demo.tape` reproduzierbar, im README eingebettet
- CI-YAML liegt, Frisch-Clone-Probe dokumentiert, ressourcen-abhängige Tests sauber markiert statt gelöscht
- `twine check` grün, PyPI-Anleitung liegt, README-Umstellung als markierter Folgeschritt vorbereitet
- Launch-Texte vollständig, im Ton der Schreibregeln, als DRAFT markiert
- "foreign task"-Satz ersetzt (EN und DE)
- `pytest`, `ruff`, `mypy` grün; Pflicht-Dependencies weiterhin null

## Explizit nicht in dieser Session

PyPI-Upload, Repo public, Release-Tag, jedes Posten oder Mailen (alles Davids Schalter, siehe Checkliste). Keine neuen Analyse-Features. Und die Launch-Texte werden nicht "optimiert", bis sie klingen wie Werbung — wenn ein Satz nach Marketing klingt, ist er falsch, siehe Schreibregeln.
