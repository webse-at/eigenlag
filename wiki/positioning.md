# Positionierung

## Das Problem

Ein Data-Engineer stellt einen Airflow-DAG auf stündlich. Nach zwei Wochen laufen die Jobs eine halbe Stunde zu spät, nach vier Wochen zwei Stunden, nach drei Monaten ist die Pipeline einen halben Tag hinterher und niemand weiß, wann das angefangen hat. Die übliche Reaktion ist, Worker hinzuzufügen. Die Verspätung wächst weiter.

Der Grund ist strukturell. Wenn Lauf k auf Lauf k-1 wartet, gibt es einen Kreis über die Zeitachse. Dieser Kreis hat eine Umlaufzeit, und schneller als diese Umlaufzeit kann die Pipeline nicht takten, egal mit wie vielen Workern. Die Umlaufzeit ist der Max-Plus-Eigenwert λ.

## Was heutige Tools zeigen

Airflow, dbt, Dagster und jedes Observability-Tool darüber zeigen die **Latenz eines Laufs**: den kritischen Pfad durch den DAG. In der Demo-Pipeline sind das 5,5 Stunden. Die naheliegende Schlussfolgerung ist, dass Läufe sich überlappen dürfen und deshalb jeder Takt möglich ist, solange man genug Worker hat.

Diese Schlussfolgerung ist falsch, und zwar in einer Weise, die kein Tool aufdeckt. Die Demo-Pipeline kann nicht schneller als alle 4,4 Stunden takten. Bei einem Takt von 3 Stunden wächst die Verspätung um exakt 1,4 Stunden pro Lauf, für immer.

Der kritische Pfad und die nachhaltige Taktzeit sind zwei verschiedene Zahlen, und die zweite kennt niemand.

## Warum es das nicht gibt

Das Konzept ist nicht neu. Im Compilerbau heißt es RecMII, recurrence-constrained minimum initiation interval, und ist seit den Achtzigern Standard beim Modulo-Scheduling von Schleifen. In der Fertigungssteuerung und der Bahn-Fahrplanung ist die Max-Plus-Algebra etabliertes Handwerk.

In der Data-Engineering-Welt ist es unbekannt, weil die Werkzeuge dort aus der Web-Entwicklung kommen und nicht aus dem Scheduling. Die Frage "wie lange braucht ein Lauf" wird gestellt. Die Frage "wie oft darf ich starten, ohne dass es auseinanderläuft" wird nicht gestellt, weil niemand weiß, dass sie eine berechenbare Antwort hat.

## Für wen

Teams mit sub-täglich getakteten Pipelines, die inkrementelle Modelle, `depends_on_past` oder Cross-DAG-Sensoren mit Zeitversatz benutzen. Das ist keine Nische: genau das ist der Standardaufbau für alles, was Zustand über Läufe hinweg fortschreibt, also für jede Feature-Pipeline, jeden inkrementellen Warehouse-Build und jedes Modell mit Warm-Start.

Wie groß diese Gruppe tatsächlich ist, weiß derzeit niemand. Das herauszufinden ist der Zweck von Phase 1, und das Ergebnis darf auch ernüchternd ausfallen.

## Der Anspruch

Das Tool beantwortet drei Fragen, die heute unbeantwortet sind:

1. **Wie schnell darf ich takten?** λ, als harte Untergrenze.
2. **Wer ist schuld?** Der kritische Kreis, aufgelöst bis auf die einzelne Task.
3. **Was bringt es, wenn ich X ändere?** Das What-if-Ranking, das den Unterschied zwischen einer Optimierung auf dem kritischen Kreis (wirkt) und einer daneben (wirkt exakt null) sichtbar macht.

Die dritte Frage ist die wertvollste. Sie verhindert, dass jemand ein Quartal in die Optimierung eines Jobs steckt, der gar nicht der Engpass ist.

## Zwischenbewertung nach Phase 1 (Orchestrator, 2026-07-15)

Phase 1 ist abgeschlossen und hat die Ausgangsthese in einem Punkt widerlegt, in einem bestätigt und den Wert des Produkts verschoben. Das gehört festgehalten, bevor Launch-Material entsteht.

**Widerlegt: Drift als Massenphänomen in öffentlichem Code.** Die ursprüngliche Erzählung ("viele Pipelines takten schneller, als ihr Kreis erlaubt, und driften unbemerkt") findet im Korpus keinen Beleg. 176 Kern-Kandidaten von 51.789 DAGs, davon 78 % Anschauungsmaterial; Mehr-Task-Kreise: 4 Stück. Der einzige vermessene Produktions-Kandidat (Wikimedia) driftet nicht, sondern stabilisiert sich selbst, weil seine Sensoren auf die Uhr warten. Wer mit der Angst-Erzählung launcht, wird von den eigenen Zahlen widerlegt.

**Bestätigt und stärker geworden: die Wissenslücke.** Niemand kann die Taktfrage beantworten, auch dort nicht, wo die Struktur sie aufwirft. Der stärkste Einzelbefund des Projekts ist der Wikimedia-Sweep: 30 DAGs laufen im Median länger als ihr Takt, 29 davon driften nicht — ein Werkzeug, das nur Laufzeit gegen Schedule hält, produziert 29 Fehlalarme auf einen echten Fall. Die Zahl ist gemessen, an Produktionsdaten, und sie trägt.

**Verschoben: wo der Wert sitzt.** Weniger im λ-Wert selbst, mehr in drei Dingen: (1) der **Unterscheidung** Strukturproblem vs. Kapazitätsproblem — "diese Optimierung bringt exakt null, weil sie nicht auf dem Kreis liegt" spart echtes Geld; (2) der **Fehlalarm-Vermeidung** — die 29-von-30-Zahl ist das Verkaufsargument gegen naive Monitoring-Regeln; (3) dem **CI-Gate** — eine neue Cross-Run-Kante fällt im Review nicht auf, ein Gate rechnet nach. Der dbt-Winkel (7k+ inkrementelle Models im Korpus: Kreis bekannt, Takt unbekannt, kein Tool verbindet beides) ist unbesetztes Terrain.

**Die ehrliche Marktfrage bleibt offen.** Der Korpus kann den Markt nicht messen: die interessanten Fälle liegen in privaten Produktions-Repos, und genau das sagen unsere eigenen Daten (Konstruktoren-Kapselung bei Wikimedia vs. deren Fehlen im öffentlichen Korpus). Das ist Argument und Ausrede zugleich — entscheidbar nur durch echte Nutzer. Empfehlung: nach Session 009 (CLI auf echter Metadaten-DB) das Werkzeug an 2–3 echte Teams bringen, bevor weiter poliert wird.

**Größtes Produktrisiko** (aus Session 005, `math.md` Abschnitt 9): Pipelines mit Uhr-Synchronisation liefern gemessene Dauern, die bereits das Ergebnis des eingeschwungenen Zustands sind. Ein Analyzer, der das ignoriert, produziert genau die Fehlalarme, die er anderen vorwirft. Die Sensor-Markierung (Spec 008) ist die erste Antwort; sie muss im Report sichtbar bleiben.

## Der Use-Case: wann der verborgene Schmerz akut wird (2026-07-15)

Der Schmerz ist nicht abwesend, er ist falsch etikettiert. Wikimedia zahlt jede Stunde 48 Minuten Verspätung als eingeschwungenen Zustand — niemand hat das entschieden, es fühlt sich nicht wie ein Problem an, sondern wie eine Eigenschaft der Pipeline. Latente Schmerzen verkauft man nicht durch Aufklärung über die Krankheit, sondern an den Momenten, wo die latente Frage akut wird. Vier solche Momente, in absteigender Kauf-Stärke:

1. **Takt-Wechsel.** "Können wir von täglich auf stündlich?" Terminiert, budgetiert, heute ohne Entscheidungsgrundlage — man migriert und schaut. λ beantwortet die Frage vorher. Haupt-Pitch.
2. **Optimierung, beide Richtungen.** Vorwärts: das What-if-Ranking verhindert, dass ein Quartal in einen Task fließt, der λ um exakt null ändert. Rückwärts (Headroom): "Reserve: 83 %" heißt "viermal so oft laufen, ohne etwas anzufassen" — eine Fähigkeit, von der das Team nicht wusste, dass es sie hat. Verkauft Können statt Angst.
3. **Incident.** Nach dem ersten Drift-Vorfall will das Team eine Leitplanke: das CI-Gate. Der einzige Trigger mit Wiederkehr-Charakter (Analyzer = Diagnose, Gate = Gewohnheit).
4. **Fehlalarm-Müdigkeit.** Die 29-von-30-Zahl. Schwächster Kauf-Trigger, bester Content-Aufhänger: macht neugierig statt zu belehren.

**Konsequenz für den Launch:** Der Post führt mit Trigger 4 (die interessante Geschichte), und die Kommentare zeigen, auf welchen der vier Momente die Leute anspringen. Diese Information ist der eigentliche Ertrag des Experiments — wichtiger als Stars oder Installs. Danach ist entscheidbar, ob das Produkt ein Takt-Wechsel-Werkzeug, ein Optimierungs-Ranking oder ein CI-Gate ist.

**Abbruchkriterium, weil es Davids Bedingung von Anfang an war:** Zieht keiner der vier Momente (keine Nachfragen, keine Installs, Kommentare bleiben bei "nett"), hat der Use-Case den Markttest nicht bestanden. Dann wird geparkt, nicht schöngeredet.
