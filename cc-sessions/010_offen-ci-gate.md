# Session 010 — CI-Gate: `eigenlag check --against REF`

**Phase 2, vorletzter Baustein.** Das Gate beantwortet die Frage, die ein interessiertes Team als erstes stellt: "Kann das in unsere CI?" Es vergleicht λ vor und nach einem Diff und schlägt an, wenn eine Änderung die Pipeline über ihre Taktgrenze hebt — bevor sie gemerged ist. Dazu zwei kleine Report-Korrekturen aus der Abnahme 009a, die vor dem ersten fremden Leser erledigt sein müssen.

## Vorher lesen

- `wiki/log.md`, Eintrag **009a** — dort steht die Gate-Metrik-Entscheidung und der What-if-Befund
- `wiki/decisions.md`: ADR-002, ADR-007, ADR-018 (JSON-Keys aus 009 sind die Schnittstelle)
- `eigenlag/report.py` (`compose()` liefert die stabilen Keys, auf denen das Gate aufsetzt)
- Der ursprüngliche Auftrag verlangt wörtlich: "Exit-Code != 0 plus fertiger PR-Kommentar-Text, wenn eine neue Cross-Run-Kante lambda über den konfigurierten Schedule hebt"

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **Gate-Metrik ist Punkt-λ gegen Punkt-λ** — als **ADR-022** festschreiben (Begründung aus 009a übernehmen: dieselbe Statistik vor und nach dem Diff, deterministisch, Differenzen attributierbar; der systematische MC-Bias — λ_p50 > Punkt-λ wegen Konvexität des Maximums plus Lognormal-Mean über Median — kürzt sich beim Gleiches-mit-Gleichem-Vergleich heraus). Monte Carlo läuft **nie** gegen Schwellen; das Gate zeigt es nicht einmal an, sonst diskutiert jedes Code-Review zwei Zahlen statt einer.
2. **Der Vergleichsstand kommt aus einem temporären Worktree**, nicht aus `git show` je Datei: `git worktree add --detach <tmp> REF`, denselben relativen Pfad parsen, Worktree entfernen. Damit funktionieren Multi-File-Konstrukte (Factories, Konstruktoren, Selbst-Referenz-Ziele) auf beiden Ständen identisch. Read-only, kein Netz, keine Mutation am Arbeits-Repo — und ausdrücklich kein `git checkout` im Nutzer-Tree.
3. **Dauern im Gate: Struktur-Modus ist der Default.** CI-Umgebungen haben in der Regel keine Metadaten-DB. Ohne Quelle rechnet das Gate mit uniformen Dauern (1.0) und sagt das im Kommentar-Text ("Struktur-Vergleich: λ in Task-Einheiten"). Eine **neue** Cross-Run-Kante ist auch strukturell sichtbar — genau der Fall, den der Auftrag nennt. `--db`/`--assume-duration` wie in `analyze`, dann ist λ in Sekunden.
4. **Fail-Regel, Default exakt nach Auftrag:** Exit ≠ 0, wenn (a) eine neue Cross-Run-Kante hinzukam **und** (b) λ_nachher > T (aus Schedule oder `--period`). Zwei schärfere Modi als Flags: `--fail-on-new-edge` (jede neue Kante, unabhängig von T — für Teams, die Kanten bewusst budgetieren) und `--max-increase PROZENT` (λ-Wachstum deckeln, auch ohne neue Kante). Kein Fail ohne benennbare Ursache: der Kommentar-Text zeigt **die** Kante mit Datei:Zeile, die den Ausschlag gab.
5. **Exit-Codes:** 0 = bestanden (auch: keine DAGs in beiden Ständen — ein Repo ohne DAGs darf keine CI brechen, mit Hinweis), 1 = Bedienfehler, 3 = Gate ausgelöst. Die 2 bleibt bei `analyze` reserviert; die Räume überlappen nicht.
6. **Kein GitHub-API-Call, niemals.** Das Gate schreibt den PR-Kommentar als Markdown auf stdout bzw. `--comment-file`. Das Posten ist Sache des CI-Jobs des Nutzers. Ein Beispiel-Workflow (GitHub Actions YAML) kommt in die Doku, wird aber nicht ausgeführt — das Tool bleibt ohne Netz-Seiteneffekte.

## Auftrag

### 1. `eigenlag check`

```
eigenlag check PFAD --against REF
  [--db URL | --assume-duration SEK]     sonst Struktur-Modus
  [--dag-id ID] [--period SEK] [--statistic ...] [--since ...]
  [--fail-on-new-edge] [--max-increase PROZENT]
  [--comment-file PFAD] [--json]
```

Ablauf: beide Stände parsen (Worktree-Mechanik aus Vorentscheidung 2), je DAG λ und Kanten-Menge vergleichen. Verschwundene DAGs, neue DAGs, umbenannte Tasks: benennen, nicht raten (ein neuer DAG mit Kreis und sub-T-λ ist ein Fail nach Default-Regel, sein "Vorher" ist "existierte nicht").

### 2. Der PR-Kommentar

Markdown, deutsch, fertig zum Einfügen. Aufbau: einzeiliges Urteil zuerst (bestanden / ausgelöst mit Grund), dann je betroffenem DAG: λ vorher → nachher (mit Einheit bzw. "Task-Einheiten" im Struktur-Modus), T und Quelle, **die auslösende Kante mit Datei:Zeile und Signal-Art**, der neue kritische Kreis (kondensiert und aufgelöst, ADR-002), und ein What-if-Hinweis, welche Änderung den Fail beheben würde (die billigste Kante/Task-Änderung aus dem Ranking, die λ wieder unter T bringt — wenn keine existiert, das sagen). Modellgrenzen-Fußzeile wie im Report, gekürzt auf zwei Sätze.

JSON-Ausgabe (`--json`) mit denselben Feldern über `compose()`-artige Trennung — Text und JSON aus einer Quelle, wie in 009.

### 3. Report-Korrekturen aus 009a (vor dem Gate-Teil erledigen, eigener Commit)

1. **Null-Delta-Kompaktierung:** What-if-Zeilen mit Veränderung ±0 werden zu einer Sammelzeile ("N weitere Szenarien ändern λ nicht: M Kreis-Gleichstände, K Kanten außerhalb des kritischen Kreises"). Die Detail-Zeilen bleiben in `--json` vollständig.
2. **Schlusssatz angleichen:** Der Satz "das Ranking zeigt deshalb nur Kreis-Tasks und Cross-Kanten" beschreibt das Verhalten falsch (es zeigt alle Cross-Kanten). Entweder Verhalten auf den Satz ändern oder den Satz auf das Verhalten — Entscheidung des Implementers, mit einer Zeile Begründung im Log. Beides zusammen (Kompaktierung plus korrekter Satz) muss am Flaggschiff-Report sichtbar sein: derselbe Aufruf wie in 009a, vorher 15 Rausch-Zeilen, nachher lesbar.

### 4. Tests

- **Fixture-Repo mit echter Git-Historie**, im Test erzeugt (`git init`, drei Commits): v1 ohne Cross-Run-Kante, v2 mit `depends_on_past=True` bei sub-täglichem Schedule (Gate löst aus, Kommentar enthält Kante mit Zeile), v3 Kante wieder entfernt (Gate besteht). Dazu: unveränderter Stand (besteht), DAG gelöscht (benannt, besteht), neuer DAG mit Kreis (Fail nach Default-Regel).
- **Realistische Inhalte statt Spielzeug:** die Fixture-DAG-Files sind gekürzte Varianten des Flaggschiff-Files (echte Struktur, kontrollierte Historie). Der Korpus hat keine Historie (shallow Clones), also wird die Historie synthetisch gebaut — aber der Code darin ist echt.
- Worktree-Mechanik: Test belegt, dass das Nutzer-Repo unangetastet bleibt (`git status` vorher/nachher identisch) und der temporäre Worktree auch bei Exceptions verschwindet.
- Exit-Code-Matrix als Tabelle getestet.
- Beide Report-Korrekturen mit Tests gepinnt.

### 5. Verifikation

1. Gate end-to-end auf dem Fixture-Repo: alle drei Historien-Stände, Kommentar-Texte vollständig ins Log.
2. **Selbst-Anwendung als Negativ-Probe:** `eigenlag check` auf einem Repo ohne DAGs (dieses hier) → Exit 0 mit Hinweis, kein Absturz.
3. Der korrigierte Flaggschiff-Report (What-if kompaktiert) im Log, direkt neben der 009a-Fassung.

## Akzeptanz

- `eigenlag check --against REF` läuft via pipx-Installation, Exit-Codes nach Matrix
- Default-Fail-Regel entspricht wörtlich dem Auftrag (neue Kante **und** λ > T), schärfere Modi per Flag
- PR-Kommentar nennt die auslösende Kante mit Datei:Zeile und einen Behebungs-Hinweis
- ADR-022 (Punkt-λ als Gate-Metrik) in `wiki/decisions.md`
- Beide 009a-Report-Korrekturen umgesetzt, am Flaggschiff sichtbar
- GitHub-Actions-Beispiel in der Doku (`docs/` oder README-Abschnitt), nicht ausgeführt
- `pytest`, `ruff`, `mypy` grün; Pflicht-Dependencies weiterhin null

## Explizit nicht in dieser Session

Packaging/README/Sprachfassung (011 — dort fällt mit der Launch-Route auch die Entscheidung über einen englischen Report), dbt (nach Feedback), jedes automatische Posten in PRs oder sonstige Netz-Seiteneffekte.
