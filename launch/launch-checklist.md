# Davids Schalter-Reihenfolge. Jeder Schritt ist Davids Klick, keiner passiert automatisch.

Je Schritt: was er bewirkt, und was schiefgehen kann. Reihenfolge einhalten;
insbesondere kein Posten, bevor Install-Weg und CI sichtbar funktionieren.

- [x] **0. Sicherheits- und Historien-Review vor dem Schalter (erledigt, 2026-07-21).**
  - Secret-Scan der vollen Historie: `gitleaks detect` (v8.30.1) über alle 52 Commits
    (34 MB), no leaks found; `gitleaks dir` über den Arbeitsstand, no leaks found.
  - Commit-Messages (`git log --all`): durchgehend technisch und deutsch, keine Secrets,
    keine dritten Personen außer der öffentlich gedachten Wikimedia-Referenz. Deutsche
    Messages und der im Log sichtbare Orchestrator/Implementer-Workflow gehen mit dem
    Schalter öffentlich; das ist für ein Build-in-public-Repo bewusst und unbedenklich.
  - `.claude/`: nur `settings.json`, untracked und in keinem Commit der Historie
    (`git log --all -- .claude/` leer). Enthält lokale Pfade und einen Verweis auf ein
    anderes Projekt (jukeep.com). Über den Visibility-Schalter wird es nicht öffentlich,
    solange es nicht committet ist. Erledigt: `.claude/` steht seit der Abnahme 014a
    in `.gitignore` (Commit 00428cf), ein versehentliches `git add` zieht es nicht mehr rein.

- [x] **Entschieden (David, 2026-07-21): `wiki/positioning.md` bleibt unverändert im Repo,**
  inklusive aller historischen Fassungen. Begründung: jede Glättung bliebe in der Historie
  sichtbar und wäre die schlechtere Geschichte; offene Produktfindung mit Abbruchkriterium
  passt zur Build-in-public-Identität. Falls jemand das Dokument in einem Thread zitiert,
  liegt die vorbereitete Antwort unten in dieser Datei ("Prepared response: positioning.md").

- [ ] **1. Repo public stellen** (GitHub → Settings → Danger Zone → Change visibility).
  Bewirkt: Links in Case-Study und Launch-Texten funktionieren, CI kann laufen.
  Schiefgehen: der Secret- und Historien-Check aus Schritt 0 ist die Voraussetzung; er ist
  erledigt (no leaks). Scan-Artefakte unter `scan/` und `wikimedia/` sind bewusst
  öffentlich gedachte Belege.

- [ ] **2. Ersten CI-Lauf prüfen** (Actions-Tab).
  Bewirkt: der Badge im README wird grün und belegt die Suite auf 3.12 und 3.14.
  Auslöser: die Übersetzungs-Commits dieser Session (`wikimedia/case.md`, `wiki/math.md`,
  `wiki/signals.md`, README/CLAUDE) sind der erste Push nach dem Public-Schalter und
  triggern den CI-Lauf — dieser Lauf ist der Badge-Beleg. Ein manueller Re-Run tut es
  ebenso.
  Schiefgehen: der Lauf ist auf einem nackten Runner nie zuvor gelaufen — die
  Frisch-Clone-Probe (wiki/log.md, Session 013) bildet ihn nach, aber erst dieser
  Lauf beweist ihn. Das 3.12-Bein war lokal nicht prüfbar (Server hat nur 3.14).
  Wenn rot: Log lesen, Fix committen, nicht am YAML raten.

- [ ] **3. PyPI-Upload** (Anleitung: `docs/pypi-release.md`).
  Bewirkt: `pipx install eigenlag` funktioniert weltweit.
  Schiefgehen: eine hochgeladene Version ist unveränderlich; bei Fehlern 0.1.1 statt
  Überschreiben. Token nie ins Repo.

- [ ] **4. README-Install-Zeile umstellen** (`git apply launch/readme-pypi-install.patch`, committen, pushen).
  Bewirkt: der Quickstart zeigt den PyPI-Weg statt `git+https`.
  Schiefgehen: der Patch vor Schritt 3 angewandt wäre eine Lüge im README.

- [ ] **5. Release v0.1.0 taggen** (`git tag v0.1.0 && git push --tags`, dann GitHub-Release
  mit dem redigierten Text aus `launch/release-notes-v0.1.0.md`).
  Bewirkt: zitierbarer Stand; das Release ist der Anker für den Reddit-Post.
  Schiefgehen: Tag auf den Commit NACH der README-Umstellung setzen, sonst zeigt
  das Release den git+https-Quickstart.

- [ ] **6. About und Topics setzen** (GitHub → About-Zahnrad).
  Beschreibung: "Computes the sustainable minimum cycle time (max-plus eigenvalue λ)
  of Airflow pipelines: the hard lower bound more workers cannot beat."
  Topics: `airflow`, `data-engineering`, `scheduling`, `pipeline`, `max-plus`.
  Bewirkt: Auffindbarkeit über GitHub-Suche und Topic-Seiten.
  Schiefgehen: wenig; Tippfehler in Topics kosten nur Sichtbarkeit.

- [ ] **7. Ein paar Tage Ruhe.** Bewirkt: Zeit, um Issues aus Schritt 1–6 zu fangen
  (kaputter Badge, PyPI-Rendering, GIF-Link), bevor Publikum draufschaut.
  Schiefgehen: nichts; der einzige Fehler wäre, diesen Schritt zu überspringen.

- [ ] **8. Wikimedia-Mail senden** (redigiert aus `launch/wikimedia-mail.md`; Kanal
  vorher verifizieren: Mailing-Liste oder Phabricator).
  Bewirkt: Heads-up vor der Veröffentlichung, Chance auf Korrekturen und die eine
  "echtes Team"-Rückmeldung.
  Schiefgehen: keine Antwort ist ein legitimes Ergebnis; 3–4 Tage warten, dann Schritt 9
  auch ohne Antwort.

- [ ] **9. Reddit-Post** (redigiert aus `launch/reddit-post.md`, r/dataengineering).
  Bewirkt: der eigentliche Markttest; die Kommentare zeigen, welcher Trigger-Moment
  zieht (wiki/positioning.md).
  Schiefgehen: Self-Promotion-Regeln des Subreddits vorher prüfen; auf Kritik nüchtern
  und schnell antworten, Korrekturen offen zugeben.

- [ ] **10. Airflow-Slack** (redigiert aus `launch/airflow-slack.md`, passenden Kanal wählen).
  Bewirkt: Reichweite in der Kern-Community.
  Schiefgehen: falscher Kanal wirkt wie Spam; im Zweifel erst die Kanal-Beschreibung lesen.

- [ ] **11. Woche 1: Issues und Kommentare schnell beantworten.** David leitet weiter,
  der Orchestrator bereitet Antworten vor.
  Bewirkt: die Kommentar-Auswertung ist der eigentliche Ertrag des Experiments,
  wichtiger als Stars oder Installs.
  Schiefgehen: zieht keiner der vier Trigger-Momente, gilt das Abbruchkriterium
  aus wiki/positioning.md — parken, nicht schönreden.

---

## Prepared response: positioning.md

Falls jemand in einem Thread aus `wiki/positioning.md` zitiert (die Planungsnotizen mit
"Kauf-Stärke", "Haupt-Pitch", Kommentare-als-Messinstrument), ist das die vorbereitete
Antwort — Davids Stimme, bei Bedarf anpassen, nicht eskalieren, nicht rechtfertigen:

> Yes, those are my product-discovery notes, in the open like everything else in this
> repo — including the criterion for when I'd shelve the project. I'd rather you read my
> honest planning than a polished version of it. The numbers in the post stand on their
> own either way; every one has a permalink.

Ein Follow-up nur, wenn konkret nachgefragt wird. Nicht löschen, nicht umschreiben —
das Dokument ist Teil des Build-in-public-Inhalts.
