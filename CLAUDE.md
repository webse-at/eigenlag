# CLAUDE.md

## Projekt

**eigenlag** — Rekurrenz-Analyzer für Daten-Pipelines. Berechnet für Airflow/dbt die nachhaltige minimale Taktzeit (Max-Plus-Eigenwert λ) und damit die harte Untergrenze, unter die ein Schedule nicht gedrückt werden kann, egal wie viele Worker laufen.

Kernthese: Cross-Run-Abhängigkeiten (`depends_on_past`, `wait_for_downstream`, `ExternalTaskSensor` mit `execution_delta`, dbt `is_incremental()`) erzeugen Zyklen über die Zeitachse. λ dieses Graphen ist die Taktgrenze. Schedule < λ bedeutet unbegrenzt wachsende Verspätung. Kein Data-Tool berechnet das heute.

## Harte Regeln

1. **Wiki ist Wahrheit.** `wiki/index.md` vor jeder Session lesen. Bei Konflikt zwischen Gedächtnis und Wiki gewinnt das Wiki. Bei Konflikt zwischen Wiki und Code gewinnt der Code, dann Wiki korrigieren.
2. **Kein Wert ohne Herleitung.** Jede Zahl im Output (λ, Drift, p95) muss aus einer im Code nachvollziehbaren Rechnung stammen. Keine Konstanten, die "ungefähr stimmen". Wenn ein Testwert nicht reproduzierbar ist, ist der Test kaputt, nicht die Erwartung anzupassen.
3. **Der Prototyp ist Ground Truth, aber nur er.** `wiki/maxplus_pipeline.py` liegt vor und wurde am 2026-07-13 laufen gelassen: λ = 4.40 h, Drift 1.40 h/Lauf bei T = 3.0, kritischer Kreis `monitor → monitor` (aufgelöst: `core → features → retrain → score → monitor`). Diese Werte sind verifiziert und sind Test-Pins. Was NICHT im Prototyp steht (Howard, Monte Carlo, Parser-Semantik), ist damit auch nicht belegt und braucht eigene Herleitung. Siehe `wiki/decisions.md` ADR-001.
4. **AST statt Regex.** Jede Signal-Erkennung in Airflow-DAG-Files läuft über `ast`. Regex erzeugt False Positives (Kommentare, Strings, Doku-Blöcke) und ist für den Marktbeweis wertlos, weil jede Zahl anfechtbar wird.
5. **Signal ist DAG-scoped, nicht File-scoped.** Ein File kann mehrere DAGs enthalten. Ein `depends_on_past=True` in DAG A macht DAG B im selben File nicht zum Cross-Run-DAG.
6. **Jeder Treffer ist belegbar.** Jede Zeile in `scan_results.csv` referenziert Repo, Datei und Zeilennummer. Ein Treffer, den David nicht in 30 Sekunden auf GitHub nachschlagen kann, zählt nicht.
7. **GitHub-API-Fehler werden geloggt, nicht geraten.** Bei Rate-Limit, 404, Timeout: strukturiert in `scan_errors.jsonl`, Lauf geht weiter. Niemals ein Ergebnis erfinden oder einen Repo-Skip stillschweigend verschlucken.
8. **Resume-fähig by default.** Jeder Scan-Schritt schreibt Zwischenstand auf Disk. Ein Abbruch nach 180 von 200 Repos darf nicht bedeuten, dass 180 Clones neu gezogen werden.
9. **Direkt auf `main`.** Keine Feature-Branches. Commits klein und thematisch.
10. **Keine schweren Dependencies.** Erlaubt: `numpy`, optional `sqlalchemy`. Alles andere braucht Begründung im Plan. Kein `networkx`, kein `pandas` im Core (Scanner darf `pandas` für CSV, wenn es sich rechnet, aber `csv` aus der stdlib reicht).

## Rollen

**David ist Auftraggeber, Claude ist Orchestrator.** Der Orchestrator schreibt Session-Specs nach `cc-sessions/`, pflegt Wiki und STATUS, prüft die Ergebnisse der Implementer-Sessions gegen die Spec. Er implementiert nicht selbst. Implementierung passiert in eigenen CC-Sessions, die je genau eine Spec abarbeiten.

## Pflichtschritte Ende jeder Implementer-Session

```
□ 1. STATUS.md aktualisieren (Stand, was lief, was offen ist)
□ 2. STATUS.md — Abschnitt "Hinweise für nächste Session":
       offene Entscheidungen, ungelöste Fragen, was der Orchestrator prüfen soll
□ 3. wiki/log.md — Session-Eintrag append (was gemacht, was gemessen, was überrascht hat)
□ 4. wiki/changelog.md — Eintrag bei Feature-Complete
□ 5. wiki/decisions.md — ADR bei Architektur-Entscheidung
□ 6. Betroffene Wiki-Seiten korrigieren, wenn der Code von ihnen abweicht
□ 7. Tests grün (`pytest`), Beleg im Session-Log (Ausgabe pasten, nicht behaupten)
□ 8. `ruff check .` und `ruff format --check .` grün
□ 9. mypy grün (`mypy eigenlag/`), sobald das Package existiert
□ 10. Commit + Push auf main
□ 11. Spec-File von `_offen-` auf `_done-` umbenennen
□ 12. Nächsten Session-Prompt in cc-sessions/ ablegen, falls der Orchestrator
       das nicht schon getan hat
```

## Plan vor Code, aber verhältnismäßig

Eine Session, deren Spec die erwarteten Werte bereits nennt, braucht kein Plan-Dokument, das die Spec paraphrasiert. Sie braucht **die Tests zuerst**: Erwartung hinschreiben, rot sehen, implementieren, grün sehen.

Ein Plan ist dann gefragt, wenn die Spec eine Entscheidung offen lässt (Datenstruktur, Algorithmus-Wahl, Grenzfall-Semantik). Dann gilt: die offene Entscheidung benennen, einen Vorschlag mit Begründung machen, kurz OK abwarten. Kein Optionen-Katalog, keine drei Phasen, wo eine reicht.

Wo die Spec keine Entscheidung offen lässt, wird sie ausgeführt, nicht neu verhandelt.

## Verifikation

**Grün heißt gemessen, nicht geglaubt.** Für jede Behauptung der passende Beleg:

| Behauptung | Beleg |
|---|---|
| "Scanner läuft" | Ausgabe eines echten Laufs über echte Repos, mit Trefferzahlen |
| "λ stimmt" | Testlauf mit gepastetem `pytest`-Output |
| "CLI installierbar" | `pipx install .` gelaufen, `eigenlag --help` gezeigt |
| "Kein False Positive" | Stichprobe von 10 Treffern, je Datei plus Zeile aufgelöst |

Eine Test-Suite verifiziert Code-Korrektheit, nicht Feature-Korrektheit. Für den Scanner heißt das: die Stichprobe ist Pflicht, nicht die Kür.

## Stack

Python 3.12+ (Server: 3.14.4), `ast` aus der stdlib für Parsing, `numpy` für den Mathe-Kern, `sqlalchemy` optional für die Airflow-Metadaten-DB, `httpx` oder `urllib` für GitHub-API. Tests mit `pytest`. Lint und Format mit `ruff`. Typing strikt, mit `mypy` geprüft. Packaging über `pyproject.toml`, Installation über `pipx`.

## Code-Konventionen

- Typing überall, kein `Any` ohne Kommentar mit Begründung
- Reine Funktionen im Mathe-Kern, keine IO-Seiteneffekte
- Englisch im Code, Deutsch in der Nutzer-Ausgabe der CLI und in dieser Doku
- Tests neben dem Source-File (`karp.py` und `karp_test.py`), nicht in einer `tests/`-Sammelhalde
- Keine defensiven Try-Except-Schichten zwischen eigenen Funktionen. Validierung nur an Systemgrenzen: GitHub-API, gefremdete DAG-Files, Metadaten-DB, User-Input
- Fremde DAG-Files sind Systemgrenze: ein `SyntaxError` beim Parsen eines geklonten Repos ist erwartbar und wird geloggt, nicht geworfen

## Kommunikation

Direkt, kompakt, kein Fluff. Keine AskUserQuestion-Formulare, Fragen als Klartext im Chat. Plan vor Multi-Step, dann kurz OK abwarten. Bei Unklarheit fragen statt raten, und wenn eine Quelle prüfbar ist, erst die Quelle prüfen und dann handeln.

## Anti-Pattern (vermeiden)

1. **Werte behaupten statt herleiten.** λ = 4.40 ist verifiziert, weil der Prototyp gelaufen ist und die Zahl von Hand nachgerechnet wurde (Cross-Kante `monitor(k-1) → core(k)` plus Intra-Pfad `1.1 + 0.9 + 1.6 + 0.5 + 0.3` bei Kreislänge 1). Genau diese Sorgfalt gilt für jede weitere Zahl. Ein Test, der grün wird, weil die Erwartung an die Ausgabe angepasst wurde, beweist nichts.
2. **Regex-Scanning "nur schnell zum Anschauen".** Die Zahlen aus dem Scan sind Launch-Content und werden öffentlich behauptet. Eine False-Positive-Quote, die niemand kennt, macht sie unbrauchbar.
3. **`ExternalTaskSensor` pauschal als Cross-Run zählen.** Ohne `execution_delta` oder `execution_date_fn` zeigt er auf denselben Logical Date, ist also eine Intra-Run-Kante zwischen zwei DAGs, keine Rekurrenz.
4. **Permutations-Enumeration für den kritischen Kreis.** Bei mehr als etwa 12 Knoten explodiert das. Howard-Policy-Iteration ist der richtige Algorithmus und liefert den Kreis direkt mit.
5. **Sub-täglich am Cron-String raten.** `0 */6 * * *` ist sub-täglich, `@daily` nicht, `timedelta(hours=4)` schon, `0 0 * * *` nicht. Das gehört in eine getestete Funktion, nicht in ein Inline-`if`.

## Aktueller Stand

Siehe `STATUS.md`.
