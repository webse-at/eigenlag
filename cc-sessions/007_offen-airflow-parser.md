# Session 007 — Airflow-Parser: vom DAG-File zur Pipeline

**Phase 2, erster Schritt nach dem Mathe-Kern. Das hier ist der eigentliche Wert des Produkts** — der ursprüngliche Auftrag sagt über den Parser-Layer wörtlich "hier Sorgfalt". Der Kern rechnet richtig (004, abgenommen mit Brute-Force-Kreuzvergleich). Was fehlt, ist der Weg von echtem DAG-Code zu einer `Pipeline`, ohne dass auf diesem Weg Semantik erfunden wird.

**Die Leitregel dieser Session:** Der Parser darf weniger wissen, als im File steht, aber nie mehr. Jede Stelle, die er nicht sicher auflösen kann, wird als Warnung mit Datei und Zeile gemeldet, nicht geraten. Ein λ aus einem halb geratenen Graphen ist schlimmer als kein λ, weil es Autorität ausstrahlt, die es nicht hat. Weglassen einer Kante ist dabei die sichere Richtung: λ bleibt eine gültige Untergrenze (`wiki/math.md`, Abschnitt 8).

## Vorher lesen

- `wiki/signals.md` (Stand nach 006, Signale A–G)
- `wiki/math.md`, Abschnitte 4 und 8–9
- `wiki/decisions.md`: ADR-006 (periods im Zyklusmittel), ADR-007 (kein Kreis → None), ADR-018 (G eigene Klasse), ADR-019 (Teilpfad-Fälle tragen das Produkt)
- `eigenlag/model.py` (die Ziel-Struktur), `scanner/analyze.py` (was der Scanner kann und was nicht)

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **`scanner/schedule.py` wandert nach `eigenlag/schedule.py`**, der Scanner importiert ab dann aus dem Package. Sie ist stdlib-only (ADR-010, geprüft), und die Abhängigkeits-Richtung muss Produkt ← Scanner sein, nie umgekehrt: der Scanner ist Wegwerf-Code, das Package nicht. Import-Update im Scanner plus Tests gehören zur Session.
2. **Der Parser teilt mit dem Scanner die Definition, nicht die Extraktion.** `scanner/analyze.py` beantwortet "kommt Signal X in DAG Y vor, Datei:Zeile" — für die Marktzahl reicht das. Der Parser braucht mehr: **welche Task** das Signal trägt, wohin ein Sensor zeigt, welchen Wert `execution_delta` hat. Die Scanner-`Signal`-Struktur (kind/file/lineno, keine Task-Zuordnung) gibt das nicht her, und sie darum aufzubohren würde den abgenommenen Phase-1-Stand destabilisieren. Also: eigene Extraktion in `eigenlag/parse_airflow.py`. Die Konsistenz der beiden sichert ein Test: auf den Scanner-Fixtures (`scanner/fixtures/`) muss die Menge der Signal-Arten pro DAG bei beiden identisch sein. Laufen sie auseinander, ist einer von beiden falsch, und `wiki/signals.md` entscheidet, welcher.
3. **Dauern sind nicht Teil dieser Session** (Roadmap 008). Der Parser liefert Struktur; eine getrennte Funktion `to_pipeline(parsed, durations)` heiratet sie mit Dauern. Für Tests und Korpus-Lauf gilt einheitlich Dauer 1.0 je Task — damit sind λ-Aussagen dieser Session **Struktur-Aussagen** (in Einheiten "Tasks auf dem Kreis pro Periode"), keine Zeit-Aussagen, und genau so werden sie berichtet.

## Auftrag

### 1. `eigenlag/parse_airflow.py`

Eingabe: ein Python-File (oder Verzeichnis). Ausgabe pro DAG:

```python
@dataclass(frozen=True)
class ParsedDag:
    dag_id: str | None            # None, wenn nicht statisch auflösbar — nicht raten
    file: str
    lineno: int
    schedule_expr: str | None
    period_s: float | None        # aus eigenlag/schedule.py
    tasks: tuple[str, ...]
    intra: tuple[tuple[str, str], ...]
    cross: tuple[ParsedCrossEdge, ...]   # CrossEdge plus Herkunft (file:line, Signal-Art)
    warnings: tuple[Warning_, ...]       # alles, was erkannt, aber nicht modelliert wurde
```

Task-Erkennung: Operator-Instanziierungen mit statischem `task_id`, `@task`-dekorierte Funktionen, `EmptyOperator` eingeschlossen. Kanten: `>>` und `<<` (auch gekettet und mit Listen), `set_upstream`/`set_downstream`, `chain(...)`. TaskGroups werden aufgelöst (Prefix-Namespace wie in Airflow: `gruppe.task`).

**Nicht statisch auflösbar → Warnung, nicht raten.** Die drei häufigsten Fälle aus dem Korpus, alle als Fixture:
- Tasks in Schleifen mit f-String-`task_id` (`f"load_{i}"`): Task wird als `load_{i}?` mit Warnung `dynamic_task_id` erfasst, Kanten daran verfallen mit Warnung.
- Dynamic Task Mapping (`.expand(...)`): als **eine** Task erfasst, Warnung `task_mapping` (die Parallel-Instanzen teilen den Slot im Graphen, für λ ist das konservativ korrekt, solange die Dauer die der ganzen Mapping-Stufe ist).
- `dag=` aus Variablen, die nicht im File definiert sind: DAG-Zuordnung wie im Scanner (Spec 002): ein DAG im File → zuordnen mit `inferred`, mehrere → Warnung, keine Zuordnung.

### 2. Die Übersetzungstabelle Signal → Kante (der Kern der Session, als Tests zuerst)

| Signal | λ-Kante | Begründung |
|---|---|---|
| A `depends_on_past=True` an Task t | `CrossEdge(t, t, 1)` | Task wartet auf ihre eigene Vorgänger-Instanz |
| A in `default_args` | Selbst-Kante für **jede** Task des DAG (Operator-Ebene überschreibt) | Airflow-Vererbungssemantik |
| B `wait_for_downstream=True` an t | zusätzlich zu A: `CrossEdge(d, t, 1)` für jeden **direkten** Downstream d von t | t(k) wartet auf t(k−1) **und** dessen direkte Nachfolger; nur direkte, so ist Airflow definiert |
| C `ExternalTaskSensor` mit `execution_delta`, Ziel im Parse-Satz, gleiches T, `delta/T` ganzzahlig ≥ 1 | `CrossEdge(ziel_task, sensor_task, delta/T)`, Ziel-Tasks namespaced `dag_id.task_id` | die einzige Kante mit periods > 1; genau der Fall aus ADR-006 |
| C mit `execution_delta`, aber: Ziel nicht im Parse-Satz, T verschieden, oder `delta/T` nicht ganzzahlig | **keine Kante**, Warnung `sensor_not_modeled` mit dem konkreten Grund | verschiedene Takte sind im Ein-Perioden-Modell nicht darstellbar; lieber Untergrenze als erfundene Kante |
| C mit `execution_date_fn` | keine Kante, Warnung `sensor_dynamic_offset` | Rückgabewert statisch nicht bestimmbar (`signals.md`) |
| D `include_prior_dates=True` | keine Kante, Warnung `include_prior_dates` | "irgendein früherer Lauf reicht" ist schwächer als "der vorige muss fertig sein"; eine Kante würde λ fälschlich heben |
| F `prev_*_success`-Templates | **keine λ-Kante**, aber als Befund gemeldet | siehe unten, wird ADR-020 |
| G `max_active_runs=1` | `CrossEdge(s, q, 1)` für jede Senke s und jede Quelle q des DAG | Lauf k startet erst, wenn Lauf k−1 komplett fertig ist; auf DAG-Ebene ergibt das λ = Makespan, konsistent mit ADR-019 |

**Zu F, und das wird ADR-020 in dieser Session:** Für die **Marktzahl** zählt F stark (ADR-011): die Struktur "liest den erfolgreichen Vorlauf" existiert. Für **λ** erzeugt F keine Kante, denn das Template rendert einen Zeitstempel zur Laufzeit und **wartet nicht** — es gibt keine Scheduling-Abhängigkeit, die den Takt begrenzt. Ein Task mit `prev_start_date_success` startet pünktlich und liest schlimmstenfalls veraltete Daten. Das ist ein Korrektheits-, kein Durchsatz-Problem. Marktzahl und λ-Modell messen zwei verschiedene Dinge, und der Parser meldet F deshalb als Befund ("Datenabhängigkeit ohne Wartesemantik"), ohne die Taktgrenze anzufassen. Diese Divergenz gehört explizit ins ADR und in den Report-Text, sonst wirft sie uns später jemand als Inkonsistenz vor.

Jeder Tabellen-Fall ist ein Test, **bevor** implementiert wird (Tests-zuerst wie in 004). Dazu die Fixtures aus den drei Nicht-auflösbar-Fällen.

### 3. Validierung am Korpus (die Clones liegen schon da)

Über die DAG-Files der **176 Kern-Kandidaten** und der **473 G-only-Kandidaten** aus `scan/v2/scan_results.csv` (Clones unter `data/repos/`, nichts neu klonen):

1. **Parse-Quote:** Wie viele Files parsen ohne Fehler, wie viele DAGs entstehen, Warnungs-Verteilung nach Art. Jede Zahl mit Nenner ins Log.
2. **Konsistenz Parser ↔ Scanner:** pro DAG die Signal-Arten vergleichen. Abweichungen einzeln auflösen — jede ist entweder ein Parser-Bug, ein Scanner-Bug oder eine bewusste Modell-Differenz (F!), und die Auflösung steht im Log.
3. **Offener Punkt aus Abnahme 004:** Karp, Howard und (bei ≤ 8 Kreis-Knoten) Brute-Force auf **jedem** kondensierten Graphen aus dem Korpus, Dauer 1.0 je Task. Alle müssen übereinstimmen. Das ist der Kreuzvergleich auf echten Graphen statt Zufallsgraphen, der seit der 004-Abnahme aussteht.

### 4. Die Teilpfad-Jagd (Auflage aus ADR-019)

Aus den geparsten Kern-Kandidaten diejenigen DAGs identifizieren, bei denen der kritische Kreis ein **echter Teilpfad** ist: λ < Critical Path bei uniformen Dauern, also strukturell, unabhängig von echten Laufzeiten. Das sind die DAGs, bei denen ein Laufzeit-Dashboard die Taktfrage **nicht** beantworten kann — der Falltyp, der das Produkt trägt.

Deliverable: die Liste (Repo, DAG, Permalink, Kreis vs. Critical Path in Task-Anzahl), dazu **ein** durchgerechneter Fall im Log: kondensierter Kreis, aufgelöster Pfad (ADR-002), λ und Critical Path nebeneinander, mit dem expliziten Vorbehalt "uniforme Dauern, Struktur-Aussage". Wenn der Korpus keinen einzigen solchen Fall hergibt, ist **das** das Ergebnis, es wird gemeldet und nicht durch Aufweichen der Kriterien repariert — dann wissen wir, dass der Produkt-Fall in öffentlichem Code selten ist, und diskutieren die Konsequenz beim nächsten Orchestrator-Review.

## Akzeptanz

- Übersetzungstabelle vollständig als Tests, Tests zuerst geschrieben (rot → grün im Log belegt)
- Konsistenz-Test Parser ↔ Scanner auf den gemeinsamen Fixtures grün; Korpus-Abweichungen einzeln aufgelöst
- Parse-Quote über die 649 Kandidaten-DAG-Files im Log, mit Warnungs-Verteilung
- Karp = Howard (= Brute-Force wo anwendbar) auf allen kondensierten Korpus-Graphen, Zahl der geprüften Graphen im Log
- Teilpfad-Liste liegt vor oder ihr Fehlen ist als Befund dokumentiert
- ADR-020 (F: Marktzahl ja, λ-Kante nein) in `wiki/decisions.md`, `signals.md` um die λ-Spalte ergänzt
- `eigenlag/schedule.py` umgezogen, Scanner-Importe angepasst, alle 214+ Tests grün
- `ruff`, `mypy` grün; der Kern bleibt bei null Laufzeit-Dependencies

## Explizit nicht in dieser Session

Dauern aus der Metadaten-DB (008), CLI und Report-Formatierung (009), Monte Carlo (009), CI-Gate (010). Auch nicht: der Import-Check gegen falsche `DAG`-Klassen im **Scanner** (ADR-Kandidat aus 006a, eigene Scanner-Session) — der Parser braucht ihn trotzdem von Anfang an: ein File, dessen `DAG` nicht aus `airflow` importiert ist, wird nicht geparst. Das kostet hier nichts, weil die Extraktion neu entsteht, und verhindert den 330-Zeilen-Fehler aus 006 im Produkt.
