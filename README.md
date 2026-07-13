# eigenlag

Rekurrenz-Analyzer für Daten-Pipelines. Berechnet, wie schnell eine Airflow- oder dbt-Pipeline überhaupt takten kann, bevor sie unaufholbar zurückfällt.

## Die Bäckerei

Eine Bäckerei backt Sauerteigbrot. Jeden Morgen geht ein Teil des gestrigen Ansatzes in den heutigen Teig, der Rest wird gefüttert und braucht zwölf Stunden, bis er wieder triebfähig ist.

Der Bäcker kann zehn Öfen kaufen und zwanzig Leute einstellen. Er kann trotzdem nicht öfter als alle zwölf Stunden backen, weil der Sauerteig auf sich selbst wartet. Die Kapazität ist nicht der Engpass, der Kreislauf ist es.

Jede Pipeline mit `depends_on_past`, `wait_for_downstream` oder einem inkrementellen dbt-Model hat so einen Sauerteig. Nur weiß niemand, wie lange er reifen muss.

## Was das Tool rechnet

Wenn Lauf k auf Lauf k-1 wartet, entsteht ein Kreis über die Zeitachse. Dieser Kreis hat eine Umlaufzeit λ, den Max-Plus-Eigenwert des Abhängigkeitsgraphen. λ ist die kürzeste Taktzeit, die die Pipeline dauerhaft halten kann.

Läuft der Schedule mit Periode T:

- **T ≥ λ**: stabil, Verspätungen klingen ab.
- **T < λ**: jeder Lauf startet `λ - T` später als der vorige. Die Verspätung wächst linear und unbegrenzt. Mehr Worker ändern daran nichts.

Heutige Tools zeigen den kritischen Pfad eines einzelnen Laufs. Das ist eine andere Zahl. In der Demo-Pipeline beträgt der kritische Pfad 5,5 Stunden, die nachhaltige Taktzeit aber 4,4 Stunden. Wer auf drei Stunden taktet, verliert 1,4 Stunden pro Lauf, für immer, und kein Dashboard sagt ihm warum.

## Stand

Das Projekt ist in Arbeit. Es gibt noch kein installierbares CLI. Der Mathe-Kern ist als Prototyp verifiziert (`wiki/maxplus_pipeline.py`), die Portierung in ein Package läuft.

Der Plan steht in [wiki/roadmap.md](wiki/roadmap.md), der aktuelle Stand in [STATUS.md](STATUS.md). Die Theorie hinter λ ist in [wiki/math.md](wiki/math.md) hergeleitet.

## Verwandtes

Das Konzept ist im Compilerbau seit Jahrzehnten Standard: RecMII, das recurrence-constrained minimum initiation interval beim Modulo-Scheduling von Schleifen. In der Fertigungssteuerung und der Fahrplanung ist die Max-Plus-Algebra etabliertes Handwerk. In der Data-Engineering-Welt ist sie unbekannt.
