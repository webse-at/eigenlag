# Wiki — eigenlag

Diese Sammlung ist die kanonische Wahrheit über das Projekt. Vor jeder Session lesen. Wenn der Code dem Wiki widerspricht, gewinnt der Code und das Wiki wird korrigiert.

## Seiten

| Seite | Inhalt |
|---|---|
| [positioning.md](positioning.md) | Was das Tool ist, für wen, und warum es das heute nicht gibt |
| [math.md](math.md) | Max-Plus-Eigenwert, Kondensation, Karp, Howard. Die Theorie hinter λ |
| [signals.md](signals.md) | Die Cross-Run-Signale A bis G, exakt definiert. Grundlage für Scanner und Parser |
| [../wikimedia/case.md](../wikimedia/case.md) | Der erste echte Fall: λ an einer Produktions-Pipeline, mit PromQL und Permalink zu jeder Zahl |
| [architecture.md](architecture.md) | Aufbau des Packages, Schichten, Datenfluss |
| [roadmap.md](roadmap.md) | Session-Plan Phase 1 und Phase 2, mit Abhängigkeiten |
| [decisions.md](decisions.md) | ADRs. Jede Architektur-Entscheidung mit Begründung |
| [log.md](log.md) | Session-Protokoll. Was passiert ist, was gemessen wurde |
| [changelog.md](changelog.md) | Feature-Historie |

## Der Kern in fünf Sätzen

Eine Pipeline, deren Lauf k auf Lauf k-1 wartet, hat einen Zyklus über die Zeitachse. Über die Max-Plus-Algebra lässt sich dieser Zyklus als Matrix darstellen, deren Eigenwert λ dem maximalen Zyklusmittel entspricht. λ ist die minimale Taktzeit, die die Pipeline dauerhaft halten kann. Läuft der Schedule mit Periode T < λ, wächst die Verspätung pro Lauf um genau λ - T, unbegrenzt und unabhängig von der Worker-Anzahl. Mehr Rechenleistung hilft nicht, weil der Engpass die Abhängigkeitsstruktur ist, nicht die Kapazität.

## Das Bäckerei-Beispiel

Eine Bäckerei hat einen Sauerteig-Ansatz. Jeden Morgen wird ein Teil des gestrigen Ansatzes für den heutigen Teig verwendet, der Rest wird neu gefüttert und muss zwölf Stunden reifen, bevor er wieder brauchbar ist. Der Bäcker kann zehn Öfen kaufen und zwanzig Bäcker einstellen. Er kann trotzdem nicht öfter als alle zwölf Stunden Brot backen, weil der Sauerteig auf sich selbst wartet. Die zwölf Stunden sind λ. Jede Pipeline mit `depends_on_past` hat einen Sauerteig, nur weiß niemand, wie lange er reifen muss.

## Referenzwerte (verifiziert)

Seit Session 005 gibt es dazu einen **gemessenen** Referenzfall aus der Produktion:
`wdqs_streaming_updater_reconcile_hourly` bei Wikimedia läuft im Stundentakt (T = 3600 s) mit
λ = 3598,4 s, also 1,6 Sekunden Reserve, bei 48 Minuten dauerhafter Verspätung. Herleitung,
PromQL und Permalinks in [../wikimedia/case.md](../wikimedia/case.md).

Der Prototyp [maxplus_pipeline.py](maxplus_pipeline.py) liegt vor und wurde am 2026-07-13 ausgeführt. Seine Ausgabe ist die Ground Truth für die Tests in Phase 2:

| Größe | Wert |
|---|---|
| λ (nachhaltige Zykluszeit) | 4.40 h |
| Kritischer Kreis, kondensiert | `monitor → monitor` |
| Kritischer Kreis, aufgelöst | `core → features → retrain → score → monitor` |
| Drift bei T = 3.0 h | 1.40 h pro Lauf, gemessen und theoretisch (λ - T) |
| Critical Path eines Einzellaufs | 5.5 h |
| What-if: retrain 1.6 → 0.8 | λ = 3.60 h |
| What-if: Kante `monitor → core` entfernt | λ = 2.50 h |
| What-if: core 1.1 → 0.55 | λ = 3.85 h |

λ ist von Hand nachvollziehbar: Die Cross-Kante `monitor(k-1) → core(k)` speist den Intra-Pfad `core (1.1) → features (0.9) → retrain (1.6) → score (0.5) → monitor (0.3)`, Summe 4.4, Kreislänge 1, also Zyklusmittel 4.4. Siehe [decisions.md](decisions.md) ADR-001.
