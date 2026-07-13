# Mathe: Max-Plus-Eigenwert als Taktgrenze

Diese Seite ist die Referenz für den Mathe-Kern. Sie steht bewusst vor dem Code, damit eine Implementierung gegen sie geprüft werden kann und nicht umgekehrt.

## 1. Warum überhaupt Max-Plus

Eine Task startet, wenn **alle** ihre Vorgänger fertig sind. Das ist ein Maximum. Danach dauert sie ihre Laufzeit. Das ist eine Addition. Systeme, deren Dynamik nur aus Maximum und Addition besteht, sind in der Max-Plus-Algebra linear, obwohl sie in der normalen Algebra hochgradig nichtlinear sind.

Die Max-Plus-Algebra ersetzt die üblichen Operationen:

| Klassisch | Max-Plus | Bedeutung hier |
|---|---|---|
| `a + b` | `a ⊕ b = max(a, b)` | Warten auf den langsamsten Vorgänger |
| `a · b` | `a ⊗ b = a + b` | Laufzeit an die Startzeit anhängen |
| `0` (neutral zu +) | `ε = -∞` | Kante existiert nicht |
| `1` (neutral zu ·) | `e = 0` | Keine Verzögerung |

Startet Task i im Lauf k zur Zeit `x_i(k)`, gilt

```
x(k) = A ⊗ x(k-1)      also      x_i(k) = max_j ( A[i][j] + x_j(k-1) )
```

Das ist eine lineare Rekursion. Genau wie eine klassische lineare Rekursion einen Eigenwert hat, der das asymptotische Wachstum bestimmt, hat diese hier einen Max-Plus-Eigenwert λ, der die asymptotische **Taktzeit** bestimmt.

## 2. Was λ bedeutet

λ erfüllt `A ⊗ v = λ ⊗ v` für einen Eigenvektor v. Im Klartext: Nach einer Einschwingphase verschiebt sich jeder Lauf um exakt λ gegenüber dem vorigen. λ ist die Zykluszeit, die das System aus eigener Kraft halten kann.

Der zentrale Satz (Cuninghame-Green): **λ ist gleich dem maximalen Zyklusmittel des Graphen.**

```
λ = max über alle Kreise C in G:  ( Summe der Kantengewichte in C ) / ( Anzahl der Kanten in C )
```

Der Kreis, der dieses Maximum realisiert, ist der **kritische Kreis**. Er ist der Engpass. Jede Verkürzung einer Task, die nicht auf dem kritischen Kreis liegt, ändert λ um exakt null.

## 3. Konsequenz für den Schedule

Läuft die Pipeline mit Schedule-Periode T:

- **T ≥ λ**: stabil. Verspätungen aus einem Lauf klingen ab.
- **T < λ**: instabil. Jeder Lauf startet um `λ - T` später als der vorige. Die Verspätung wächst linear und unbegrenzt.

Die Drift ist damit exakt `λ - T` pro Lauf. Kein Näherungswert, sondern der asymptotische Grenzwert. Mehr Worker ändern λ nicht, weil λ eine Eigenschaft der Abhängigkeitsstruktur ist und nicht der Kapazität. Das ist die ganze Pointe des Tools: es trennt Kapazitätsprobleme (durch Worker lösbar) von Strukturproblemen (nicht durch Worker lösbar).

## 4. Kondensation: vom Task-Graph zur Cross-Run-Matrix

Der volle DAG hat viele Tasks, aber nur wenige haben eine Kante in den nächsten Lauf. Die Matrix A wird deshalb nicht über alle Tasks aufgespannt, sondern nur über die **Cross-Run-Knoten**, also Tasks, die eine Kante nach k+1 besitzen.

Für zwei Cross-Run-Knoten `quelle` und `ziel` gilt

```
Abar[ziel][quelle] = längster Pfad im Intra-Run-DAG
                     vom Eintrittspunkt, den die Cross-Run-Kante aus `quelle` speist,
                     bis einschließlich `ziel`
```

Existiert kein solcher Pfad, ist der Eintrag `ε = -∞`. Der längste Pfad ist wohldefiniert, weil der Intra-Run-Graph azyklisch ist. Er wird per Topologie-Sortierung in linearer Zeit berechnet, nicht per Enumeration.

Diese Kondensation ist der Grund, warum das Verfahren auch bei DAGs mit hunderten Tasks schnell bleibt: die Eigenwert-Rechnung läuft auf einer Matrix in der Größenordnung der Cross-Run-Knoten, typischerweise einstellig bis niedrig zweistellig.

## 5. Karp

Karps Algorithmus berechnet das maximale Zyklusmittel exakt in `O(V · E)`:

```
D[0][v] = 0 für einen Startknoten s, sonst -∞
D[k][v] = max über Kanten (u → v):  D[k-1][u] + w(u, v)      für k = 1..n

λ = max über v mit D[n][v] > -∞:
      min über k = 0..n-1 mit D[k][v] > -∞:
        ( D[n][v] - D[k][v] ) / ( n - k )
```

Karp liefert λ zuverlässig, aber **nicht** den kritischen Kreis. Den muss man separat rekonstruieren.

## 6. Kritischer Kreis: Howard, nicht Enumeration

Die naive Suche nach dem kritischen Kreis testet alle Kreise. Das ist bei mehr als etwa zwölf Knoten nicht mehr machbar, weil die Anzahl der Kreise faktoriell wächst.

**Howard-Policy-Iteration** ist die richtige Antwort. Sie ist in der Praxis nahezu linear und liefert den kritischen Kreis direkt als Nebenprodukt:

1. Wähle für jeden Knoten eine beliebige ausgehende Kante. Das ist die Policy π. Der Graph aus lauter Policy-Kanten hat pro Komponente genau einen Kreis, weil jeder Knoten Ausgrad eins hat.
2. Berechne für die aktuelle Policy den Kreismittelwert η und die Bias-Werte v (Potenziale relativ zum Kreis).
3. Suche eine Kante `(u → w)`, die sich lohnt: `w(u, w) + v[w] - η > v[u]`. Wenn es keine gibt, ist die Policy optimal und η = λ.
4. Wechsle zu dieser Kante und gehe zu Schritt 2.

Bei Terminierung ist der Kreis in der finalen Policy der kritische Kreis. Howard ist damit sowohl schneller als auch informativer als Karp. Karp bleibt trotzdem im Code, als unabhängige Zweitmeinung: beide müssen dasselbe λ liefern, und ein Test pinnt das. Zwei unabhängige Verfahren, die übereinstimmen, sind der beste verfügbare Beleg für die Korrektheit, solange keine externe Referenz existiert.

## 7. Stochastik

Task-Dauern sind keine Konstanten. Aus der Airflow-Metadaten-DB werden pro Task die historischen Laufzeiten gezogen und ein Lognormal-Fit gerechnet (Lognormal, weil Laufzeiten positiv und rechtsschief sind). Monte Carlo über diese Verteilungen liefert eine Verteilung von λ, aus der `λ_p50` und `λ_p95` abgelesen werden.

`λ_p95` ist die eigentlich interessante Zahl: sie beantwortet, ob der Schedule auch an einem schlechten Tag hält. Ein Schedule, der gegen `λ_p50` stabil ist und gegen `λ_p95` nicht, ist eine Pipeline, die einmal pro Monat aus dem Ruder läuft und die niemand erklären kann.

## 8. Grenzen des Modells

Ehrlich benannt, damit niemand mehr hineinliest, als drinsteht:

- **Unbeschränkte Parallelität angenommen.** λ ist eine Untergrenze. Bei zu wenigen Workern ist die reale Taktzeit größer. Das Tool sagt "nicht schneller als λ", nicht "λ ist erreichbar".
- **Deterministische Dauern im Kern.** Die Stochastik sitzt außen herum als Monte Carlo, nicht in der Max-Plus-Rechnung selbst.
- **Retries, Sensor-Poking, `max_active_runs` und Pool-Limits** sind nicht modelliert. Sie können die reale Taktzeit nur erhöhen, nie senken, also bleibt λ eine gültige Untergrenze.
- **Kein Cross-Run-Kante heißt kein λ.** Ein DAG ohne Rekurrenz hat keinen Kreis über die Zeitachse. Das Ergebnis ist dann nicht "λ = 0", sondern "nicht anwendbar". Der Unterschied gehört sauber in den Output, sonst liest jemand eine Null als Entwarnung.
