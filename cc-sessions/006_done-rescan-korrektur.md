# Session 006 — Re-Scan mit neuer Definition, Fall-Korrektur

**Warum diese Session existiert:** Session 005 hat zwei Regeln in den Scanner gebracht, die die Korpus-Zahlen aus 003 doppelt veralten lassen. ADR-015 (DAG-Konstruktoren) hat allein bei Wikimedia die gefundenen DAGs verfünffacht (71 → 345). ADR-016 (Signal G, `max_active_runs=1`) ist eine neue Kandidaten-Klasse, die 003 gar nicht kannte. Die alten Zahlen (51.426 DAGs, 176 Risiko-Kandidaten) sind damit vor jeder öffentlichen Behauptung neu zu rechnen. Dazu kommt die Fall-Korrektur aus ADR-017.

## Vorher lesen

- `wiki/decisions.md`: **ADR-015, ADR-016, ADR-017** — die drei Entscheidungen, die diese Session umsetzt
- `wiki/signals.md` (Stand nach 005, mit Signal G)
- `scan/report.md` — der alte Report, dessen Zahlen ersetzt werden
- `STATUS.md`

## Ausgangslage (geprüft bei der Abnahme von 005)

- `scanner/analyze.py` **kann** bereits Konstruktoren (ADR-015, Vorlauf ab Zeile 620) und Signal G (ab Zeile 291). Die Erkennung ist nicht dein Auftrag, sie liegt vor und ist getestet.
- `scanner/report.py` **kennt G nicht**: das `STRONG`-Set endet bei `prev_run_success`, es gibt keine G-Spalte, und `risk_candidate` ignoriert G. Hier sitzt die Arbeit.
- Die 1692 Clones liegen unter `data/repos/`. **Nicht neu klonen** (Regel 8). Die per-Repo-State-Files aus 003 enthalten aber Ergebnisse nach alter Definition — sie müssen invalidiert werden, die Clones bleiben.

## Auftrag

### 1. Zwei Risiko-Klassen statt einer (als ADR-018 festhalten)

Signal G ist eine echte Cross-Run-Kante (ADR-016), aber es ist die Kante, bei der λ = Makespan gilt und ein Laufzeit-Dashboard dieselbe Antwort gibt (ADR-017). Es darf deshalb **nicht stillschweigend ins `STRONG`-Set**, sonst springt die Quote nach oben und der erste kritische Leser sagt: "eure Risiko-Kandidaten sind DAGs, deren Laufzeit über dem Takt liegt, das zeigt mir jedes Dashboard." Gleichzeitig wäre Weglassen falsch, der Wikimedia-Fall hat gezeigt, dass die Kante real bindet.

Die Auflösung sind zwei getrennt ausgewiesene Klassen:

| Klasse | Definition | Bedeutung |
|---|---|---|
| `risk_candidate` (Kern) | mindestens ein starkes Signal aus **A, B, C, D, F** und sub-täglich | der Kreis ist ein Teilpfad, λ < Makespan möglich, kein heutiges Tool beantwortet das. **Das bleibt die Launch-Zahl**, und sie bleibt mit 003 vergleichbar, weil die Definition unverändert ist |
| `risk_candidate_g_only` | **nur** G als starkes Signal und sub-täglich | Kante real, aber λ = Makespan; beantwortbar durch Laufzeit-Monitoring. Eigene Zeile im Report, nie in die Kern-Quote gemischt |

Ein DAG mit A–F-Signal **und** G zählt in die Kern-Klasse (G wird als Spalte trotzdem ausgewiesen). Umsetzung in `report.py`: neue Spalte `sig_g_max_active_runs`, neue Spalte `risk_candidate_g_only`, `STRONG` bleibt wie es ist, Tests in `report_test.py` erweitern.

**Warum das ADR-018 werden muss:** Wir ändern die Zähl-Definition **nachdem** der erste Scan gelaufen ist. Das ist genau die Bewegung, die ADR-005 verbietet, wenn sie die Zahl aufbläst. Die Zwei-Klassen-Lösung ist die Antwort darauf: die Kern-Quote bleibt definitionsgleich und vergleichbar, die neue Klasse steht daneben, mit gemessener Begründung (Wikimedia). Der Report legt diese Änderung offen, statt sie zu verstecken.

### 2. Re-Scan über die gecachten Clones

- Analyse-State aus 003 invalidieren, Clones behalten. Ein Abbruch mitten im Lauf darf beim Neustart nicht von vorn beginnen (Regel 8), also State-Files versioniert neu aufbauen statt löschen-und-beten.
- Voller Lauf über alle 1692 Repos aus `data/candidates.jsonl`.
- **dbt nicht neu rechnen.** ADR-015 und ADR-016 sind Airflow-seitig, `analyze_dbt.py` ist unverändert. Die dbt-Zahlen aus 003 werden übernommen und im Report als übernommen gekennzeichnet.
- DAGs ohne `dag_id` (der Preis von ADR-015, bei Wikimedia 90 Stück): eigene Zeile mit leerer `dag_id` und Flag, **nicht raten**, und als eigene Zahl im Report.

### 3. Outputs nach `scan/v2/`, die alten bleiben liegen

Nichts überschreiben. Der neue Report braucht die alten Dateien für die Pflicht-Tabelle **Vorher/Nachher**:

| Größe | 003 (alt) | 006 (neu) | Ursache |
|---|---|---|---|
| DAGs gefunden | 51.426 | ? | ADR-015 findet Konstruktor-DAGs |
| Risiko-Kandidaten (Kern) | 176 | ? | mehr DAGs im Nenner **und** im Zähler, Definition unverändert |
| Risiko-Kandidaten (nur G) | — | ? | neue Klasse, ADR-016 |

**Jedes Delta muss einer Ursache zugeordnet sein.** Eine Kern-Quote, die sich ändert, obwohl die Definition gleich blieb, hat ihre Erklärung in ADR-015 (anderer Nenner und Zähler) — das ist nachzuweisen, nicht zu behaupten: ziehe eine Stichprobe von 5 neuen Kern-Kandidaten, die 003 nicht hatte, und zeige je, dass sie aus einem Konstruktor-DAG stammen.

### 4. Stichproben (drei, je zufällig gezogen)

1. **10 Kern-Kandidaten**: Permalink auflösen, Signal an Datei und Zeile, Schedule wirklich sub-täglich. Ein Falsch-Positiv → Ursache finden, korrigieren, Lauf wiederholen. Nicht die Stichprobe nachziehen.
2. **10 G-only-Kandidaten**: `max_active_runs=1` steht explizit als Literal (kein auflösbarer Ausdruck, kein Default), Schedule sub-täglich.
3. **10 signalfreie Repos** (Falsch-Negativ-Prüfung wie in 003, aber unter neuer Definition): von Hand begründen, warum kein Signal. Ein Muster, das der Scanner nicht kennt, wäre der wertvollste Fund — so wurden ADR-009 und ADR-015 gefunden.

Alle drei nach `scan/v2/sample_verification.md`, mit vollständigen Pfaden (Regel 6).

### 5. Fall-Korrektur nach ADR-017

`wikimedia/case.md` überarbeiten. Die Messarbeit bleibt vollständig erhalten, die Darstellung ändert sich:

- **"1,6 Sekunden Reserve" streichen**, überall (Zeilen 9 und 142 im aktuellen Stand). Ersatz: das System ist rückgekoppelt (Korrelation −0,504) und pendelt sich am Fixpunkt mittlere Dauer ≈ T ein. Die 1,6 s sind kein Balanceakt, sondern der eingeschwungene Zustand.
- **Ehrlich hinschreiben, was λ hier ist:** auf DAG-Ebene, ohne Task-Dauern, ist der Graph ein Knoten mit Selbst-Kante und λ ist die Laufdauer selbst. Der Fall validiert nicht die Eigenwert-Maschinerie, er belegt die These. Der Satz darf ruhig so direkt im Dokument stehen — er macht den Fall glaubwürdiger, nicht schwächer.
- **Die Überschrift wird der Sweep:** 30 DAGs mit Median-Laufzeit über dem Takt, 29 driften nicht. Das ist der Befund, der das Produkt begründet (Fehlalarm-Argument), und er gehört nach oben.
- **Ausreißer-Empfindlichkeit benennen:** der wcqs-Hänger (400.132 s) verschiebt den Mittelwert um ~560 s bei 712 Läufen. Für asymptotischen Drift ist der Mittelwert die richtige Statistik, aber ein hängender Lauf vergiftet ihn. Ein Absatz, keine Glättung.
- `wiki/index.md` ist bereits korrigiert (Orchestrator, bei der Spec-Erstellung). Prüfen, ob `wiki/log.md`-Eintrag 005 eine Richtigstellung als Nachtrag braucht — den bestehenden Eintrag **nicht** umschreiben, Historie bleibt Historie.

### 6. Report-Sprache

Der Abschnitt "Was diese Zahlen nicht sagen" aus 003 bleibt vollständig und wächst um zwei Punkte:

- **Die Definitionsänderung wird offengelegt:** Signal G kam nach dem ersten Scan dazu, mit gemessener Begründung (Wikimedia-Fall, ADR-016), und wurde als eigene Klasse ausgewiesen statt in die Kern-Quote gemischt (ADR-018). Wer das versteckt, liefert die Angriffsfläche selbst.
- **G-only heißt: Laufzeit-Monitoring reicht dort.** Der Satz steht im Report, damit ihn kein Kritiker zuerst sagt.

## Akzeptanz

- Voller Lauf über die 1692 gecachten Clones ohne Absturz und ohne Neu-Klonen. Beleg: Lauf-Output mit Repo-Zähler und Laufzeit im Session-Log.
- `scan/v2/` vollständig: `scan_results.csv` (mit `sig_g_max_active_runs` und `risk_candidate_g_only`), `scan_factories.csv`, `scan_dbt.csv` (übernommen und als übernommen markiert), `report.md`, `sample_verification.md`.
- Vorher/Nachher-Tabelle mit Ursachen-Zuordnung, inklusive der 5-Kandidaten-Stichprobe für das ADR-015-Delta.
- Alle drei Stichproben dokumentiert.
- `wikimedia/case.md` nach ADR-017 korrigiert, Messwerte unverändert.
- ADR-018 in `wiki/decisions.md`.
- Tests (`report_test.py` um G erweitert), `ruff`, `mypy` grün.

## Explizit nicht in dieser Session

Parser für Phase 2, CLI, alles hinter dem Scanner. Auch keine neue Signal-Erkennung: wenn Stichprobe 3 ein neues Muster findet, wird es dokumentiert und als ADR vorgeschlagen, aber die Regel selbst ist eine eigene Session — sonst wiederholt sich der Zirkel "Definition geändert, alle Zahlen veraltet" innerhalb derselben Session, und der Lauf war umsonst.
