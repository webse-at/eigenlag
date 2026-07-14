# Session 004 — Mathe-Kern

**Phase 2, Schritt 1.** Der bestabgesicherte Teil des Projekts, weil `wiki/maxplus_pipeline.py` als verifizierte Referenz vorliegt. Diese Session hängt nicht am Scanner und kann parallel zu Phase 1 laufen.

## Vorher lesen

- `wiki/math.md` — vollständig. Die Seite ist die Spezifikation, der Code setzt sie um.
- `wiki/maxplus_pipeline.py` — die Referenz. Vorher einmal ausführen, Ausgabe ansehen.
- `wiki/decisions.md` ADR-001, ADR-002, ADR-003

## Arbeitsweise für diese Session

**Tests zuerst, kein Plan-Dokument.** Alle erwarteten Werte stehen unten in der Tabelle, sie sind aus dem Prototyp verifiziert. Damit sind die Tests vollständig schreibbar, bevor eine Zeile Implementierung existiert. Also: Tests schreiben, rot sehen, implementieren, grün sehen. Ein Plan, der die Spec paraphrasiert, bringt hier nichts.

**Die eine offene Entscheidung**, die vor dem Code kurz zu benennen ist: **wie `periods` ins Zyklusmittel eingeht.** Der Prototyp kennt den Versatz nicht, hier gibt es also keine Vorlage. Schreib in zwei, drei Sätzen hin, wie du das Zyklusmittel definierst und warum, bevor du Karp und Howard anfasst. Alles andere in dieser Session ist Portierung und braucht keine Rückfrage.

## Auftrag

Baue `eigenlag/model.py` und `eigenlag/maxplus.py` plus Tests. **Kein Parser, keine CLI, keine DB.** Der Kern kennt weder Airflow noch dbt.

### 1. `model.py`

```python
@dataclass(frozen=True)
class Pipeline:
    durations: dict[str, float]
    intra: list[tuple[str, str]]
    cross: list[CrossEdge]

@dataclass(frozen=True)
class CrossEdge:
    src: str        # Task im Lauf k - periods
    dst: str        # Task im Lauf k
    periods: int = 1
```

Der `periods`-Versatz ist die eine Erweiterung über den Prototyp hinaus. Er ist nötig für `execution_delta = 2 * Periode` und verändert die Mathematik: **eine Cross-Kante mit Versatz n zählt im Zyklusmittel als n Kanten.** Das Zyklusmittel ist damit `Summe der Gewichte / Summe der periods`, nicht `Summe der Gewichte / Anzahl der Kanten`. Für alle `periods == 1` fällt das auf den bekannten Fall zurück, und genau das muss ein Test zeigen.

Validierung an der Systemgrenze: unbekannter Task-Name in einer Kante, negative Dauer, `periods < 1` werfen sofort. Innerhalb des Kerns wird nicht mehr geprüft.

### 2. `maxplus.py`

- **`condense(pipeline) -> tuple[Matrix, PathMap]`**
  Kondensiert auf die Cross-Run-Quellknoten. Anders als der Prototyp, der die Matrix über *alle* Jobs aufspannt: Zeilen und Spalten nur über Knoten, die tatsächlich eine ausgehende Cross-Kante haben. Das Ergebnis ist identisch (Knoten ohne ausgehende Cross-Kante können auf keinem Kreis liegen), aber die Matrix wird klein, und Karp skaliert mit `O(V·E)`.
  Längster Intra-Pfad per Topologie-Sortierung, nicht per Enumeration. Die Pfad-Rekonstruktion wird mitgeführt, sie ist der aufgelöste Kreis aus ADR-002.

- **`karp(matrix) -> float`**
  Wie in `wiki/math.md`, Abschnitt 5. Muss den `periods`-Versatz berücksichtigen.

- **`howard(matrix) -> tuple[float, list[Node]]`**
  Policy-Iteration nach `wiki/math.md`, Abschnitt 6. Liefert λ **und** den kritischen Kreis. Das ist der produktive Weg. `itertools.permutations` kommt nicht vor, siehe ADR-003 und CLAUDE.md Anti-Pattern 4.

- **`drift(lam, T) -> float`**
  `lam - T`. Trivial, aber als benannte Funktion, damit der Report keine Inline-Arithmetik enthält.

- **`simulate(pipeline, T, k) -> list[float]`**
  Die Drift-Simulation aus dem Prototyp. Sie ist der empirische Gegencheck zur analytischen Rechnung: die gemessene Drift muss gegen `λ - T` konvergieren. Das ist ein Test, kein Feature.

**Kein Cross-Run heißt kein λ.** Wenn `pipeline.cross` leer ist, gibt es keinen Kreis. Das Ergebnis ist `None` mit klarer Semantik "nicht anwendbar", nicht `0.0`. Eine Null würde als Entwarnung gelesen und wäre eine Falschaussage (siehe `wiki/math.md`, Abschnitt 8).

### 3. Tests

**Referenz-Fixture** (die Demo-Pipeline aus dem Prototyp, Werte sind verifizierte Pins):

| Erwartung | Wert |
|---|---|
| λ | 4.40 |
| Kritischer Kreis, kondensiert | `monitor → monitor` |
| Aufgelöster Pfad des Segments | `core → features → retrain → score → monitor` |
| Drift bei T = 3.0 | 1.40 pro Lauf |
| Critical Path Einzellauf | 5.5 |
| What-if retrain 1.6 → 0.8 | λ = 3.60 |
| What-if Kante `monitor → core` weg | λ = 2.50 |
| What-if core 1.1 → 0.55 | λ = 3.85 |

**Karp gegen Howard:** Auf jeder Fixture müssen beide dasselbe λ liefern. Das ist der Korrektheitsbeleg aus ADR-003.

**Simulation gegen Analytik:** `simulate` bei T = 3.0 über 20 Läufe, die Drift der letzten fünf Läufe muss `λ - T` treffen.

**Edge-Cases (aus dem Auftrag):**
- Kein Cross-Run: `cross = []` → λ ist `None`, nicht 0.
- Einzelne Selbst-Kante: ein Task mit `depends_on_past`, λ ist die Dauer dieses Tasks. Von Hand nachrechenbar.
- **Zwei-Perioden-Kreis:** dieselbe Struktur wie die Selbst-Kante, aber `periods=2`. λ muss **halbiert** sein, weil das Zyklusmittel durch 2 statt durch 1 teilt. Dieser Test ist der Beleg, dass der `periods`-Versatz wirklich in der Mathematik ankommt und nicht nur im Datentyp steht.
- Zwei disjunkte Kreise: λ ist das Maximum der beiden, und der kritische Kreis ist der größere.

**Gegen den Prototyp:** Ein Test importiert `wiki/maxplus_pipeline.py` nicht (er ist ein Skript mit Seiteneffekten beim Import), sondern repliziert dessen `DUR`, `INTRA`, `CROSS` als Fixture und pinnt die oben gelisteten Werte. Der Prototyp selbst bleibt unverändert liegen.

## Akzeptanz

- `pytest` grün, Output ins Session-Log gepastet
- λ = 4.40 reproduziert, Karp und Howard stimmen überein
- Zwei-Perioden-Test grün
- `mypy eigenlag/` grün, `ruff` grün
- Keine Dependency außer `numpy`

## Hinweis zur Sorgfalt

Wenn Howard und Karp auseinanderlaufen, ist das **nicht** ein Toleranz-Problem, das man mit einem größeren `tol` wegräumt. Es ist ein Bug in einem der beiden. Erst verstehen, dann fixen. Ein Test, der grün wird, weil die Toleranz aufgeweicht wurde, ist wertlos (CLAUDE.md, Regel 2).
