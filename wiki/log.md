# Session-Log

Chronologisch. Neue Einträge unten anhängen. Jeder Eintrag nennt, was gemacht wurde, was gemessen wurde und was überrascht hat.

---

## 000 — Orchestrierung, Doku-Skelett, Prototyp-Verifikation (2026-07-13)

**Rolle:** Orchestrator. Kein Produktiv-Code geschrieben, das ist Absicht.

**Gemacht:**
- Projekt-Skelett: `CLAUDE.md`, `STATUS.md`, `README.md`, `wiki/`, `cc-sessions/`, git-Repo initialisiert.
- Wiki angelegt: `index`, `math`, `signals`, `architecture`, `positioning`, `roadmap`, `decisions`, `log`, `changelog`.
- Session-Specs 001 bis 004 geschrieben.

**Gemessen:**

Der Auftrag beschrieb `maxplus_pipeline.py` als vorhandenen, validierten Prototyp. Zu Sessionbeginn war die Datei auf keiner Maschine auffindbar (Suche über `/home/webse`, `/mnt/data`, `/tmp`). Die Referenzwerte waren damit unbelegt, und ADR-001 stand zunächst als offener Blocker im Wiki. David hat die Datei nachgereicht (`wiki/maxplus_pipeline.py`), zwei Zustellversuche kamen nicht an, der dritte über direktes Ablegen im Ordner hat funktioniert.

Prototyp ausgeführt, Ausgabe:

```
Critical Path eines Laufs (Latenz): 5.5 h
Nachhaltige Zykluszeit: lambda = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
(a) Retrain halbieren:            lambda = 3.60 h
(b) Quality-Gate asynchron:       lambda = 2.50 h
(c) Core-Job optimieren:          lambda = 3.85 h
```

λ zusätzlich von Hand nachgerechnet: Cross-Kante `monitor(k-1) → core(k)` speist den Intra-Pfad `core (1.1) + features (0.9) + retrain (1.6) + score (0.5) + monitor (0.3) = 4.4`, Kreislänge 1, Zyklusmittel 4.4. Alle Auftrags-Referenzwerte sind damit reproduziert **und** hergeleitet. ADR-001 aufgelöst.

**Was überrascht hat:**

1. **Der kritische Kreis ist nicht der, der im Auftrag steht.** Der Auftrag nennt `core → features → retrain → score → monitor`. Im kondensierten Graphen ist der Kreis aber die Selbst-Kante `monitor → monitor`, und die genannte Kette ist der aufgelöste Intra-Pfad dieses einen Segments. Beide Beschreibungen sind korrekt, meinen aber verschiedene Objekte. Wer nur eine davon im Report zeigt, produziert Verwirrung. Daraus wurde ADR-002.

2. **Der Prototyp kondensiert nicht wirklich.** `build_Abar` spannt die Matrix über alle acht Jobs auf, nicht nur über die drei Cross-Run-Quellen (`core`, `retrain`, `monitor`). Das Ergebnis stimmt trotzdem, weil Knoten ohne ausgehende Cross-Kante auf keinem Kreis liegen können und deshalb nichts beitragen. Für den Produktions-Code ist die echte Kondensation auf die Cross-Run-Knoten trotzdem richtig, weil Karp mit `O(V·E)` skaliert und V sonst unnötig die Task-Anzahl statt der Cross-Run-Knoten-Anzahl ist.

3. **Der Prototyp kennt keinen Perioden-Versatz.** `CROSS` ist eine Liste von Paaren, der Versatz ist implizit immer 1. Der im Auftrag geforderte Test-Case "Zwei-Perioden-Kreis via `execution_delta = 2 * Periode`" lässt sich mit dieser Datenstruktur nicht ausdrücken. Der Produktions-Datentyp braucht ein Tripel `(von, nach, versatz)`, und der Versatz muss ins Zyklusmittel eingehen (eine Kante mit Versatz n zählt als n Kanten). Das ist die erste echte Erweiterung über den Prototyp hinaus und steht so in Spec 004.

---

## 000a — Remote, GitHub-Limits gemessen (2026-07-14)

**Gemessen:** `gh api rate_limit` mit Davids Token. GitHub führt **zwei** Code-Such-Kontingente:

```
search      (/search/code, klassisch) : 30 req/min
code_search (neuer Endpunkt)          : 10 req/min
core        (/repos/..., Metadaten)   : 5000 req/h
```

Die im Auftrag genannten "30 req/min" gelten nur für den klassischen `/search/code`. Der neue `code_search`-Endpunkt liegt bei 10 und würde den Scan ohne Not verdreifachen. Spec 001 schreibt den Endpunkt jetzt explizit vor.

**Kosten:** keine. Die API ist für öffentliche Repos kostenlos, der Token authentifiziert nur und hebt die Limits (ohne Token: 60 req/h und gar keine Code-Search).

**Remote:** `github.com/webse-at/eigenlag` angelegt, `main` gepusht.

**Nächste Session:** 004 (Mathe-Kern). Begründung: der einzige Teil mit verifizierter Referenz, hängt an nichts, und er ist der eigentliche Produktwert. Der Scanner liefert Marketing-Zahlen; stimmt der Kern nicht, sind die Zahlen wertlos.

---

## 004 — Mathe-Kern: model.py, maxplus.py (2026-07-14)

**Gemacht:** `eigenlag/model.py` (Pipeline, CrossEdge, Toposort mit Zyklus-Erkennung), `eigenlag/maxplus.py` (Kondensation, Karp, Howard, Drift, Simulation, Critical Path), Tests daneben, `pyproject.toml` mit ruff/mypy/pytest-Konfiguration. Kein Parser, keine CLI, keine DB. Der Kern kennt weder Airflow noch dbt.

**Die eine offene Entscheidung** aus der Spec, `periods` im Zyklusmittel: `Summe der Gewichte / Summe der periods`, hergeleitet über die Zustandserweiterung. Steht als ADR-006 im Wiki.

**Gemessen — Kern gegen Prototyp, dieselbe Demo-Pipeline:**

```
===== PROTOTYP (wiki/maxplus_pipeline.py) =====
Critical Path eines Laufs (Latenz): 5.5 h
Nachhaltige Zykluszeit: lambda = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
(a) Retrain halbieren:      lambda = 3.60 h
(b) Quality-Gate asynchron: lambda = 2.50 h
(c) Core-Job optimieren:    lambda = 3.85 h

===== KERN (eigenlag/) =====
Critical Path eines Laufs: 5.5 h  (ingest -> dq -> core -> features -> retrain -> score -> reports)
lambda (Karp)   = 4.40 h
lambda (Howard) = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
```

Die drei What-if-Werte sind als Tests gepinnt und grün (3.60 / 2.50 / 3.85).

**Gemessen — Test-Suite, Lint, Typen:**

```
$ .venv/bin/python -m pytest -q
...................................                                      [100%]
35 passed in 0.03s

$ .venv/bin/ruff check . && .venv/bin/ruff format --check .
All checks passed!
5 files already formatted

$ .venv/bin/mypy eigenlag/
Success: no issues found in 5 source files
```

**Gemessen — Mutations-Test für `periods`.** Die 35 Tests waren beim ersten Lauf grün, was für sich genommen kein Beleg ist. Also gegengeprüft: in einer Kopie des Pakets `periods` in beiden Verfahren auf 1 gezwungen (Expansion abgeschaltet, `η · periods` durch `η` ersetzt). Ergebnis:

```
FAILED test_karp_and_howard_agree[mixed_periods]
FAILED test_karp_and_howard_agree[two_periods]
FAILED test_two_period_self_loop_halves_lambda
FAILED test_mixed_periods_cycle_mean_divides_by_the_sum_of_periods
4 failed, 31 passed
```

Genau die vier Perioden-Tests fallen und kein anderer. Damit ist die Frage aus STATUS ("wurde `periods` nur im Datentyp geführt?") beantwortet: nein, der Versatz kommt in der Rechnung an.

**Was überrascht hat:**

1. **Kein Kreis ist nicht dasselbe wie keine Cross-Kante.** Die Spec behandelt nur `cross == []`. Es gibt aber einen zweiten Fall: eine Cross-Kante `a(k-1) → b(k)`, bei der b nie wieder auf a zurückwirkt. Der kondensierte Graph hat dann Knoten, aber keinen Kreis, und λ ist genauso wenig definiert. Deshalb geben Karp und Howard `float | None` zurück statt `float` (ADR-007), und die Sonderbehandlung sitzt nicht im Aufrufer, wo sie jemand vergessen kann.

2. **Karp braucht keine starke Zusammenhangskomponente.** Karps Satz ist für stark zusammenhängende Graphen formuliert. Der Prototyp initialisiert `D[0][v] = 0` für **alle** v, was einer virtuellen Quelle mit Null-Kanten in jeden Knoten entspricht. Die erzeugt keinen Kreis, macht aber jeden Knoten erreichbar, und damit gilt die Formel auch für zerfallende Graphen. Der Test mit zwei disjunkten Kreisen (λ = max der beiden) belegt das, er wäre sonst der erste Kandidat für einen stillen Fehler gewesen.

3. **Parallele Cross-Kanten mit verschiedenem Versatz dürfen nicht zusammengefasst werden.** Bei der Kondensation liegt es nahe, pro Knotenpaar nur das maximale Gewicht zu behalten. Das ist falsch, sobald der Versatz unterschiedlich ist: Gewicht 6 bei Versatz 2 (Mittel 3) ist schlechter als Gewicht 4 bei Versatz 1 (Mittel 4), keine Kante dominiert die andere. Die kondensierte Matrix ist deshalb nach `(quelle, ziel, periods)` geschlüsselt, nicht nach `(quelle, ziel)`.

4. **`numpy` wurde nicht gebraucht.** Der Kern rechnet auf der kondensierten Matrix mit einstelliger Knotenzahl, Karp läuft in Millisekunden. Das Package hat damit **null** Laufzeit-Dependencies. Wenn Monte Carlo in Session 006 kommt, wird `numpy` wieder aktuell.
