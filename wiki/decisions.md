# ADRs

Jede Architektur-Entscheidung mit Begründung. Neue ADRs werden angehängt, alte nicht gelöscht, sondern bei Bedarf als abgelöst markiert.

---

## ADR-001 — Prototyp ist Ground Truth, aber nur für das, was er tatsächlich rechnet

**Status:** entschieden, 2026-07-13
**Kontext:** Der ursprüngliche Auftrag nennt Referenzwerte (λ = 4.40 h, Drift 1.40 h/Lauf bei T = 3.0) und beruft sich auf einen Prototyp `maxplus_pipeline.py`. Zu Sessionbeginn war die Datei nicht auffindbar, die Werte damit unbelegt. David hat sie nachgereicht, sie liegt jetzt unter `wiki/maxplus_pipeline.py`.

**Entscheidung:** Der Prototyp wurde ausgeführt und reproduziert alle genannten Werte. λ = 4.40 wurde zusätzlich von Hand hergeleitet: Cross-Kante `monitor(k-1) → core(k)`, Intra-Pfad `1.1 + 0.9 + 1.6 + 0.5 + 0.3 = 4.4`, Kreislänge 1. Die Werte sind damit **verifizierte Test-Pins** für Phase 2.

Die Geltung endet aber an der Grenze dessen, was der Prototyp wirklich tut:

- **Belegt:** Kondensation, Karp, Drift-Simulation, die drei What-if-Szenarien, die Demo-Pipeline.
- **Nicht belegt:** Howard-Policy-Iteration, Monte Carlo mit Lognormal-Fits, jede Parser-Semantik, jede Schedule-Klassifikation. Der Prototyp enthält davon nichts. Diese Teile brauchen eigene Herleitung und eigene Tests.

**Konsequenz:** Der Prototyp bleibt als Referenz im Repo liegen und wird nicht verändert. Er ist Fixture, nicht Bibliothek. Die Portierung entsteht neu unter `eigenlag/`, und ein Test vergleicht beide auf derselben Demo-Pipeline.

---

## ADR-002 — Der kritische Kreis wird kondensiert **und** aufgelöst berichtet

**Status:** entschieden, 2026-07-13
**Kontext:** Im kondensierten Graphen ist der kritische Kreis der Demo-Pipeline eine Selbst-Kante `monitor → monitor`. Der ursprüngliche Auftrag beschreibt ihn als `core → features → retrain → score → monitor`. Beides ist richtig, aber es sind zwei verschiedene Objekte: das eine ist der Kreis in der Cross-Run-Matrix, das andere der Intra-Run-Pfad, den das Kreis-Segment durchläuft.

**Entscheidung:** Der Report zeigt immer beides. Zuerst den kondensierten Kreis (das ist der Kreis, dessen Zyklusmittel λ ist), darunter je Segment den aufgelösten Task-Pfad.

**Begründung:** Ein Nutzer, der nur `monitor → monitor` liest, sucht in seinem DAG nach einer Selbst-Kante und findet fünf Tasks. Ein Nutzer, der nur den aufgelösten Pfad liest, versteht nicht, warum das ein Kreis ist. Erst beides zusammen ergibt eine Handlungsanweisung, und Handlungsanweisung ist der Zweck des Tools.

---

## ADR-003 — Howard ersetzt die Permutations-Suche, Karp bleibt als Kontrolle

**Status:** entschieden, 2026-07-13
**Kontext:** Der Prototyp sucht den kritischen Kreis über `itertools.permutations` über alle Knoten-Teilmengen. Das ist bei acht Demo-Jobs unproblematisch und bei realen DAGs unbrauchbar, weil die Anzahl der Permutationen faktoriell wächst.

**Entscheidung:** Der kritische Kreis kommt aus Howard-Policy-Iteration, die λ und den Kreis in einem Durchgang liefert. Karp bleibt im Package und wird in den Tests gegen Howard gestellt: beide müssen auf denselben Eingaben dasselbe λ liefern.

**Begründung:** Solange keine externe Referenz-Implementierung existiert, ist die Übereinstimmung zweier unabhängig hergeleiteter Verfahren der stärkste verfügbare Korrektheitsbeleg. Karp kostet `O(V·E)` und läuft auf der kondensierten Matrix in Millisekunden, der doppelte Aufwand ist also billig.

---

## ADR-004 — Signale sind DAG-scoped und werden per AST erkannt

**Status:** entschieden, 2026-07-13
**Kontext:** Der Vorgänger-Scanner arbeitete mit Regex. Die Scan-Zahlen sind Launch-Content und werden öffentlich behauptet.

**Entscheidung:** Erkennung ausschließlich über `ast`. Ein Signal wird dem DAG zugeordnet, in dessen Kontext es steht, nicht dem File. Ein File mit zwei DAGs liefert zwei Zeilen im CSV.

**Begründung:** Regex trifft `depends_on_past` in Kommentaren, Docstrings, README-Snippets und in `depends_on_past=False`. Jeder dieser Treffer ist ein False Positive, und ein einziger, den ein kritischer Leser findet, kippt die Glaubwürdigkeit der gesamten Statistik. Die Marktzahl ist nur so viel wert wie ihre schwächste Zeile.

---

## ADR-005 — `prev_ds` und `prev_execution_date` sind schwache Signale und zählen nicht in die Risiko-Quote

**Status:** entschieden, 2026-07-13
**Kontext:** Signal F (Prior-Run-Templates) umfasst sowohl `prev_start_date_success` als auch `prev_ds`. Semantisch sind das zwei verschiedene Dinge.

**Entscheidung:** Die `*_success`-Varianten zählen als starkes Signal. `prev_ds`, `prev_execution_date` und Verwandte werden erfasst, getrennt ausgewiesen und **nicht** in die Risiko-Kandidaten-Quote eingerechnet.

**Begründung:** `prev_ds` ist Datums-Arithmetik. Es zeigt an, dass ein Task Daten des Vorlaufs liest, erzwingt aber keine Wartesemantik und damit keine Kante, die λ hebt. Wer es mitzählt, bekommt eine größere Zahl für den Launch und einen angreifbaren Report. Die kleinere, verteidigbare Zahl ist mehr wert.

---

## ADR-006 — Der Perioden-Versatz ist die Kantenlänge im Zyklusmittel

**Status:** entschieden, 2026-07-14 (Session 004)
**Kontext:** Der Prototyp kennt keinen Versatz, jede Cross-Kante zeigt implizit auf den Vorlauf. Für `execution_delta = 2 * Periode` braucht `CrossEdge` ein `periods`-Feld, und die Spec verlangt vor dem Code eine Definition, wie es ins Zyklusmittel eingeht. Eine Vorlage gibt es dafür nicht.

**Entscheidung:**

```
Zyklusmittel = Summe der Kantengewichte / Summe der periods
```

**Begründung:** Eine Cross-Kante mit Versatz *n* ist im Max-Plus-System eine Verzögerung um *n* Perioden, das System ist damit nicht mehr erster Ordnung. Die übliche Zustandserweiterung führt es auf erste Ordnung zurück: die Kante wird durch eine Kette aus *n* Kanten der Länge 1 ersetzt, von denen die erste das Gewicht trägt und die restlichen null. Der Eigenwert des erweiterten Systems ist das maximale Zyklusmittel dieses Graphen, und das ist genau der Quotient oben. Für `periods == 1` überall fällt er auf `Summe / Kantenzahl` zurück.

**Konsequenz für die Implementierung:** Karp rechnet auf genau dieser Expansion (`_expand`), Howard rechnet das Verhältnis nativ (`w - η · periods`). Die beiden Verfahren teilen sich damit **keinen** Code für die Perioden-Behandlung und bleiben unabhängige Zweitmeinungen im Sinn von ADR-003. Ein Mutations-Test belegt das: zwingt man `periods` in beiden Verfahren auf 1, fallen genau die vier Perioden-Tests, sonst keiner.

---

## ADR-007 — Kein Kreis heißt `None`, und das steht im Rückgabetyp

**Status:** entschieden, 2026-07-14 (Session 004)
**Kontext:** `wiki/math.md`, Abschnitt 8 verlangt, dass eine Pipeline ohne Cross-Run-Kante kein λ = 0 bekommt, sondern "nicht anwendbar". Die Spec 004 schreibt die Signaturen aber als `karp(matrix) -> float` und `howard(matrix) -> tuple[float, list[Node]]`.

**Entscheidung:** Beide Funktionen geben `... | None` zurück. `None` heißt: der kondensierte Graph enthält keinen Kreis, λ ist nicht definiert.

**Begründung:** Der Fall tritt nicht nur bei `cross == []` auf, sondern auch bei Cross-Kanten ohne Rückweg (`a(k-1) → b(k)`, ohne dass b jemals wieder auf a wirkt). Das ist ein Graph mit Knoten, aber ohne Kreis, und keine Rekurrenz. Wäre die Sonderbehandlung nur ein `if pipeline.cross: ...` in der aufrufenden Schicht, würde dieser Fall stillschweigend eine falsche Zahl produzieren. Der Optional-Rückgabetyp erzwingt die Behandlung im Aufrufer und wird von mypy geprüft.
