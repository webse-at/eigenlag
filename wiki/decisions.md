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

---

## ADR-008 — Der Harvest ist zweistufig, `hits.jsonl` ist die Resume-Grenze

**Status:** entschieden, 2026-07-14 (Session 001)
**Kontext:** Die Suche läuft gegen das `search`-Kontingent (30 Requests pro Minute), die Repo-Metadaten laufen gegen `core` (5000 pro Stunde). Beide Kontingente sind getrennt, und die Zuordnung Repo zu Query entsteht erst, wenn alle Queries durch sind: dasselbe Repo wird von mehreren Queries getroffen, und `matched_queries` soll vollständig sein, nicht abhängig davon, wann der Lauf abgebrochen ist.

**Entscheidung:** Der Harvest schreibt in zwei Stufen. Stufe 1 schreibt jeden Rohtreffer der Code-Search sofort nach `data/hits.jsonl` (Query, Repo, Pfad), Fortschritt je Query und Seite nach `data/harvest_state.json`. Stufe 2 liest `hits.jsonl`, verdichtet zu einem Eintrag je Repo, holt die Metadaten und schreibt nach `candidates.jsonl` oder `rejected.jsonl`. Die Filterung passiert ausschließlich in Stufe 2.

**Begründung:** `hits.jsonl` ist die Stelle, an der ein Abbruch keinen Schaden anrichtet. Ein Repo, dessen Metadaten schon geholt sind, steht in `candidates` oder `rejected` und wird beim Neustart übersprungen; ein Repo, das nur als Rohtreffer vorliegt, kostet beim Neustart einen `core`-Request und keinen `search`-Request. Damit ist das knappe Kontingent das, was der Resume schützt. Der Preis ist eine zusätzliche Datei, deren Zeilenzahl (5520) größer ist als die Zahl der Repos (2095), weil ein Repo mehrfach getroffen wird. Das ist gewollt: `merge_hits` dedupliziert beim Lesen, und die Rohdaten bleiben nachprüfbar.

**Konsequenz:** Ein Fehler bei einem einzelnen Repo (etwa der 502 im Lauf vom 2026-07-14) führt dazu, dass das Repo weder in `candidates` noch in `rejected` landet. Der nächste Lauf sieht es deshalb wieder als offen und holt es nach. Genau das ist im Lauf passiert, ohne Zutun.

---

## ADR-009 — Task-Factories sind ein eigenes Muster und werden getrennt gezählt

**Status:** entschieden, 2026-07-14 (Abnahme Session 001)
**Kontext:** Die Stichprobe aus Session 001 enthielt `navikt/team_familie_airflow_dags`, `operators/kafka_operators.py:32-33`. Die Session hat den Fund als Falsch-Positiv eingestuft ("nur eine Funktionssignatur, kein DAG-Argument"). Das ist bei genauerem Hinsehen falsch:

```python
def kafka_consumer_kubernetes_pod_operator(
    ...,
    depends_on_past: bool = True,       # Zeile 32
    wait_for_downstream: bool = True,   # Zeile 33
    ...,
):
    """Factory function for creating KubernetesPodOperator ..."""
    return KubernetesPodOperator(...)   # Zeile 83
```

Jeder Task, der über diese Factory entsteht, trägt beide starken Signale (A **und** B). Das Signal ist echt. Es steht nur nicht dort, wo Spec 002 sucht.

**Das Problem:** Spec 002 scannt DAG-Files und ordnet Signale DAG-scoped zu. Eine Factory lebt in einem Helper-Modul, das kein DAG instanziiert. Die DAG-Files, die sie aufrufen, sehen völlig unauffällig aus. Der Scanner würde dieses Repo als signalfrei melden, obwohl es das Gegenteil ist.

**Entscheidung:** Der Scanner erkennt das Muster und zählt es **getrennt**, statt es zu übersehen oder in die Hauptquote zu mischen.

Erkennungsregel, bewusst schlicht: eine Funktion, die einen Operator instanziiert und zurückgibt (`return <Irgendwas>Operator(...)` oder `return <Irgendwas>Sensor(...)`), und die eines der Signal-Schlüsselwörter als Parameter-Default mit dem Wert `True` führt oder es an den Operator durchreicht. Treffer werden als `factory_signal` protokolliert, mit Datei und Zeile.

**Was bewusst NICHT gemacht wird:** eine interprozedurale Analyse, die Aufrufstellen zurückverfolgt und die Factory-Tasks den aufrufenden DAGs zuordnet. Das ist ein statisches Auflösungsproblem mit `**kwargs`, dynamischen Imports und Schleifen, und es ist für eine Marktzahl unverhältnismäßig.

**Konsequenz für den Report (Session 003):** Die Risiko-Quote bleibt auf die DAG-scoped Treffer bezogen und ist damit eine **Untergrenze**. Der Report muss das benennen: Repos mit Task-Factories werden mit ihrer Zahl separat ausgewiesen, mit dem Satz, dass die Hauptquote sie nicht enthält. Die Richtung des Fehlers ist die verteidigbare: wir unterschätzen, statt aufzublähen. Eine kleinere Zahl, die hält, ist mehr wert als eine größere, die kippt (vgl. ADR-005).

---

## ADR-010 — Cron wird gerechnet, nicht geraten, und `croniter` bleibt draußen

**Status:** entschieden, 2026-07-14 (Session 002). Schließt die seit Session 000 offene Dependency-Frage.
**Kontext:** Spec 002 erlaubt `croniter` als einzige Zusatz-Dependency des Scanners, um die kleinste Distanz zwischen zwei Feuerzeitpunkten zu bestimmen. Beim Implementieren zeigte sich, dass die Bibliothek dafür nicht gebraucht wird.

**Entscheidung:** `scanner/schedule.py` expandiert die fünf Cron-Felder selbst (Listen, Schritte, Bereiche, Monats- und Wochentagsnamen, die ODER-Semantik von Tag-des-Monats und Wochentag) und rechnet die kleinste Distanz über ein Referenzfenster von fünf Jahren aus. Ist sie kürzer als 24 Stunden, ist der Schedule sub-täglich.

**Begründung:** Die Herleitung ist trivial, sobald die Felder expandiert sind. Feuert ein Ausdruck an einem Tag mehr als einmal, liegt der kleinste Abstand zwangsläufig innerhalb des Tages und damit unter 24 Stunden. Feuert er höchstens einmal am Tag, ist der kleinste Abstand ein Vielfaches von 24 Stunden. Das Fenster von fünf Jahren ist nötig, damit ein jährlicher Ausdruck überhaupt zwei Feuerzeitpunkte hat und der 29. Februar zweimal vorkommt. Ausdrücke, die im Fenster nie feuern (`0 12 30 2 *`), sind `unknown`, nicht "täglich" — geraten wird nicht.

Die Alternative wäre eine Dependency für eine Funktion von rund achtzig Zeilen gewesen, deren Korrektheit ohnehin gegen die Tabelle aus `signals.md` getestet werden muss (Regel 10: keine schweren Dependencies). Der Zusatznutzen von `croniter` — echte Zeitpunkte statt nur Distanzen — wird an keiner Stelle gebraucht: der Scanner klassifiziert, er plant nicht.

**Was stattdessen hinzukam:** `pyyaml` als **Scanner**-Dependency (Extra `scanner` in `pyproject.toml`). `dbt_project.yml` und die `schema.yml` neben den Models sind verschachteltes YAML mit `+`-Präfixen; das von Hand zu parsen wäre die falsche Sparsamkeit. Der `eigenlag`-Kern bleibt abhängigkeitsfrei, `dependencies = []` steht unverändert.

---

## ADR-011 — Signal F zählt in der `*_success`-Variante als starkes Signal

**Status:** entschieden, 2026-07-14 (Session 002)
**Kontext:** `signals.md` definierte den Risiko-Kandidaten als "mindestens ein starkes Signal (A, B, C, D, E) und sub-täglicher Schedule". Derselbe Text sagt bei Signal F, die `*_success`-Varianten seien "harte Kanten" und nur `prev_ds` und `prev_execution_date` seien schwach und blieben aus der Quote heraus (ADR-005). Beides zusammen ist widersprüchlich: die Aufzählung schließt F ganz aus, die Abstufung schließt nur die schwache Hälfte aus.

**Entscheidung:** Stark sind A, B, C, D, E **und** F in den `*_success`-Varianten (`prev_start_date_success`, `prev_data_interval_start_success`, `prev_data_interval_end_success`). Schwach bleiben `prev_ds`, `prev_ds_nodash` und `prev_execution_date`; sie werden getrennt gezählt und begründen keinen Risiko-Kandidaten.

**Begründung:** Ein Task, der auf `{{ prev_start_date_success }}` zugreift, wartet per Definition auf den erfolgreichen Vorlauf. Das ist genau die Kante, die λ erzeugt. Die Aufzählung "A bis E" war eine Verkürzung aus der Zeit vor der Abstufung, kein eigener Beschluss. Die Abstufung ist die begründete Aussage, sie gewinnt.

**Konsequenz:** `signals.md` ist korrigiert. `STRONG_KINDS` in `scanner/analyze.py` enthält `prev_run_success`, nicht `prev_run_date`.

---

## ADR-012 — dbt hat keinen Schedule und darf nie in die Airflow-Risiko-Quote

**Status:** entschieden, 2026-07-14 (Überarbeitung Spec 003)
**Kontext:** Die Risiko-Definition lautet "starkes Signal **und** sub-täglicher Schedule im selben DAG". Beim Überarbeiten von Spec 003 fiel auf, dass diese Bedingung für dbt-Repos gar nicht auswertbar ist: ein dbt-Model enthält kein Schedule. Wie oft es läuft, steht in Airflow, in dbt Cloud oder in einem Cron außerhalb des Repos. `scanner/analyze_dbt.py` kennt entsprechend keinen Schedule-Begriff, und das ist richtig so.

**Entscheidung:** Airflow und dbt werden **getrennt** ausgewertet. Die Risiko-Quote wird ausschließlich über Airflow-DAGs gebildet. dbt bekommt eine eigene Tabelle mit einer eigenen Aussage: wie viele Models sind inkrementell mit `is_incremental()`, haben also eine echte Selbst-Kante.

**Begründung:** Ein dbt-Repo kann die Risiko-Bedingung konstruktionsbedingt nie erfüllen. Mischt man die 364 dbt-Repos in den Nenner, verdünnt sich die Quote um einen Faktor, der nichts mit der Wirklichkeit zu tun hat. Mischt man sie in den Zähler, behauptet man ein Risiko, das man nicht belegen kann, weil der Takt unbekannt ist. Beides wäre eine Zahl, die niemand verteidigen kann.

**Der positive Dreh:** Bei dbt kennen wir den Kreis, aber nicht den Takt. Das ist kein Mangel des Scanners, sondern genau die Lücke, die das Produkt schließt. Ein inkrementelles Model mit `is_incremental()` hat eine Selbst-Kante, und ob sein Takt darunter liegt, weiß heute niemand, weil Kreis und Takt in getrennten Systemen stehen. Der Report soll das so sagen.

---

## ADR-013 — Signal F wird an zwei weiteren Fundorten erkannt: Callable-Parameter und Modul-Variable

**Status:** entschieden, 2026-07-14 (Session 003)
**Kontext:** Der erste volle Lauf meldete nur 2 DAGs mit `prev_*_success`, obwohl die Code-Search 154 Repos für `prev_start_date_success` geliefert hatte. Die Nachsuche im Clone-Cache zeigte, warum: `scanner/analyze.py` suchte das Template ausschließlich in String-Literalen, die als Argument eines Aufrufs stehen. In echtem Code steht dieselbe Semantik an zwei anderen Stellen.

1. **Als Parametername einer Callable.** Airflow injiziert den Task-Kontext über den Parameternamen. `def get_last_execution_date(prev_start_date_success, **kwargs)` (`V-Dang/covid_pipeline`, `archive.py:3`) und `lambda prev_start_date_success: prev_start_date_success is not None` (`oxylabs/building-scraping-pipeline-apache-airflow`, `DAG/scrape.py:26`) warten genauso auf den erfolgreichen Vorlauf wie `{{ prev_start_date_success }}` im Template.
2. **Als Template in einer Modul-Variablen.** `date_last_success = '{{ prev_start_date_success }}'` (`abdurahim-dag/portfolio`, `exchange rate/solution/dags/init.py:42`), später in ein Operator-Argument interpoliert.

**Entscheidung:** Beide Fundorte zählen als Signal F, mit derselben Abstufung wie bisher (`*_success` stark, `prev_ds` und Verwandte schwach, ADR-005 und ADR-011). Die Zuordnung bleibt DAG-scoped: lexikalischer Scope, sonst der einzige DAG des Files, sonst `ambiguous_task` im Fehler-Log. Ein Helfer-Modul ohne DAG (wie `V-Dang/covid_pipeline`, `archive.py`) erzeugt kein Signal, weil es keinem DAG zuzuordnen ist.

**Begründung:** Die Semantik ist identisch, nur die Schreibweise ist eine andere. Wer nur die eine Schreibweise erkennt, misst die Verbreitung der Schreibweise und nicht die des Signals. Spec 003, Abschnitt 8b, benennt genau diesen Fall: ein Muster, das der Scanner nicht kennt, ist kein legitimer Grund für ein fehlendes Signal.

**Wirkung im vollen Lauf:** 5 zusätzliche Fundstellen in 5 Repos, davon 3 starke. Die Zahl ist klein, die Richtung ist der Punkt: der Scanner unterzählt, und diese Lücke ist jetzt geschlossen.

---

## ADR-014 — Ein `execution_delta` von null ist kein Cross-Run-Signal

**Status:** entschieden, 2026-07-14 (Session 003, aus der Stichprobe)
**Kontext:** Die Falsch-Positiv-Stichprobe auf Lauf 2 enthielt `Dat-Al/Fidai`, `airflow/dags/predict_hourly_dag.py:37`:

```python
execution_delta=timedelta(hours=0),  # Regarde la même heure d'exécution
```

Der Scanner zählte jedes gesetzte `execution_delta`, das nicht `None` war, als Signal C. Ein Versatz von null zeigt aber auf denselben Logical Date. Das ist eine Intra-Run-Kante zwischen zwei DAGs, genau der Fall, den `signals.md` ausdrücklich ausschließt, und der Autor des Codes schreibt es sogar in den Kommentar.

**Entscheidung:** `execution_delta` zählt nur, wenn der Versatz nicht null ist. Ein `timedelta(...)`-Literal wird ausgerechnet (`scanner.schedule.timedelta_seconds`); ergibt es 0, ist es kein Signal. Ein Versatz, der statisch nicht auflösbar ist (Variable, Funktionsaufruf), zählt weiterhin, weil sein einziger Zweck der Zeitversatz ist und `signals.md` diesen Fall bereits als "Cross-Run erkannt, Versatz unbekannt" führt.

**Begründung:** Ein Falsch-Positiv in einer Zahl, die öffentlich behauptet wird, kostet mehr als jede Untererfassung. Die Stichprobe hat genau dafür existiert, und die Spec verlangt bei einem Fund: Ursache beheben, Lauf wiederholen, nicht die Stichprobe nachziehen.

**Wirkung im vollen Lauf:** Cross-Run-DAGs 1335 → 1303, Risiko-Kandidaten 182 → 176.
