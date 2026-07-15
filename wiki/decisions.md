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

---

## ADR-015 — Ein Repo definiert seine eigenen DAG-Konstruktoren, und der Scanner muss sie lesen

**Status:** entschieden, 2026-07-14 (Session 005)

**Kontext:** In Wikimedias `airflow-dags` fand der Scanner 71 von 325 produktiven DAGs und **null** Cross-Run-Signale, obwohl `depends_on_past=True` dort mehrfach im Klartext steht. Der Grund ist keine Lücke in der Signal-Erkennung, sondern eine in der DAG-Erkennung: Wikimedia schreibt `with create_easy_dag(...)`, und dahinter steht eine Methode, die ein `DAG(...)` zurückgibt (`wmf_airflow_common/easy_dag.py:79`, gebunden in `search/config/dag_config.py:71`). `analyze.py` kannte nur `DAG(...)` und `@dag`.

Das ist der teuerste Befund aus Session 003: **professionelle Umgebungen kapseln ihre DAG-Erzeugung, und genau die waren für uns unsichtbar.** Die Demo-Lastigkeit der Marktzahl (78 % Beispiel-Code) erklärt sich damit auch von der anderen Seite: wir haben systematisch dort gemessen, wo unverpackt geschrieben wird, also im Lernmaterial.

**Entscheidung:** Ein Vorlauf über alle Files eines Repos sammelt seine DAG-Konstruktoren (`scanner/wrappers.py`). Konstruktor ist:

1. eine Funktion oder Methode, deren **eigener** Rumpf ein `DAG(...)` zurückgibt (ein `return` in einer verschachtelten Funktion zählt nicht für die äußere),
2. ein Modul-Alias darauf: `EasyDag = EasyDAGFactory(...).create_easy_dag`.

Aufrufe dieser Namen sind DAG-Scopes wie `DAG(...)` selbst. Das `DAG(...)` **im** Konstruktor ist die Schablone und zählt nicht mit, sonst hätte ein Repo einen DAG mehr, als es Aufrufstellen hat.

**Keine Transitivität.** Eine Funktion, die ihrerseits einen Konstruktor aufruft und dessen Ergebnis weiterreicht (`build_dag()` bei Wikimedia), wird nicht befördert. Der DAG entsteht dort, wo der Konstruktor aufgerufen wird, und **Schedule und `default_args` stehen genau an dieser Stelle**. Würde man den Aufrufer befördern, läge der Fund an der Aufrufstelle, wo beides fehlt: wir hätten die `dag_id` gewonnen und den Schedule verloren. Ein Fund ohne `dag_id` ist ehrlicher als ein Fund ohne Schedule.

**Preis:** 90 der 345 gefundenen Wikimedia-DAGs haben keine `dag_id`, weil erst der Aufrufer sie einsetzt. Sie lassen sich nicht mit Laufzeit-Metriken verknüpfen. Das ist eine offene Lücke, kein gelöstes Problem (Kandidat für Session 006: DAG-Generatoren mit Literal-Argumenten an der Aufrufstelle auflösen).

**Wirkung (Wikimedia):** DAGs 71 → 345, davon mit `dag_id` 58 → 255, mit Cross-Run-Signal 0 → 13, Risiko-Kandidaten 0 → 3.

**Kosten:** Jedes Repo wird zweimal geparst. Das ist der Preis dafür, dass der Konstruktor in einem anderen File steht als seine Aufrufe.

---

## ADR-016 — `max_active_runs=1` ist eine Cross-Run-Kante (kippt eine frühere Entscheidung)

**Status:** entschieden, 2026-07-14 (Session 005). **Ersetzt** die gegenteilige Festlegung in `signals.md` ("Was ausdrücklich kein Cross-Run-Signal ist") und den entsprechenden Punkt in `math.md`, Abschnitt 8.

**Bisherige Position:** "`max_active_runs=1` serialisiert Läufe, erzeugt aber keine Datenabhängigkeit. Begrenzt die Nebenläufigkeit, nicht die Rekurrenz. Relevant für die reale Taktzeit, nicht für λ." Dazu in `math.md`: nicht modelliert, kann die reale Taktzeit nur erhöhen, also bleibt λ eine gültige Untergrenze.

**Was sie widerlegt:** Der Wikimedia-Fall. `wdqs_streaming_updater_reconcile_hourly` läuft stündlich mit `max_active_runs=1`, und seine Läufe folgen im Mittel alle 3599,5 Sekunden aufeinander, bei einer mittleren Laufzeit von 3598,4 Sekunden. Die Läufe liegen **rückenan**. Ohne diese Kante im Modell hätte ein Teil dieser DAGs "kein Kreis, kein λ" ergeben, für Pipelines, die nachweislich nicht schneller können als ihre eigene Laufzeit.

**Die Unterscheidung, die die alte Position gemacht hat, ist die falsche.** Sie trennt Daten- von Ressourcen-Abhängigkeit. Der Max-Plus-Eigenwert kennt diesen Unterschied nicht: er sieht Kanten. `max_active_runs=1` sagt `Ende(k−1) ≤ Start(k)`, und das ist eine Kante über die Zeitachse, gleich ob sie aus einer Datei oder aus einem Scheduler-Limit kommt. Sie ist obendrein oft die **bindende** Kante, weil sie den ganzen Lauf umspannt und nicht nur einen Task.

**Was an der alten Position richtig bleibt:** λ wird durch diese Kante nie kleiner, nur größer. Die Untergrenzen-Eigenschaft geht nicht verloren, λ wird schärfer.

**Entscheidung:** `max_active_runs=1` ist ein starkes Signal (`max_active_runs`, Signal G). Nur die explizite `1` zählt. Airflows Default ist größer und lässt Läufe nebeneinander laufen; ein nicht auflösbarer Ausdruck zählt nicht.

**Wirkung (Wikimedia):** DAGs mit Cross-Run-Signal 13 → 68, Risiko-Kandidaten 3 → 8.

**Folge, die offen ist:** Der Korpus-Scan aus Session 003 (51.426 DAGs, 176 Risiko-Kandidaten) ist unter der alten Definition **und** ohne ADR-015 gelaufen. Beide Zahlen sind damit veraltet. Vor jeder öffentlichen Behauptung muss neu gescannt werden. Die Clones liegen noch (`data/repos/`, 74 GB), der Lauf ist wiederholbar.

---

## ADR-017 — Läufe werden aus der Gauge rekonstruiert, nicht aus ihr gemittelt

**Status:** entschieden, 2026-07-14 (Session 005)

**Kontext:** `airflow_dagrun_duration` ist eine Gauge, keine Verteilung. Airflow meldet am Ende eines Laufs dessen Dauer an StatsD, der Exporter hält den Wert, Prometheus scrapt ihn. Ein `avg_over_time` darüber mittelt **Scrapes, nicht Läufe**: ein Wert, der länger stehen bleibt, wiegt schwerer. Ob das dasselbe Ergebnis liefert wie ein Mittel über Läufe, hängt davon ab, wie lange jeder Wert steht, und das ist eine Annahme über den Exporter, keine Zusicherung.

**Entscheidung:** Der Lauf wird rekonstruiert (`wikimedia/runs.py`): **ein Wertwechsel der Gauge ist ein Lauf.** Der Wechsel wird je Serie gesucht, nicht auf der zusammengeführten Zeitachse, weil mehrere StatsD-Pods je einen eigenen letzten Wert halten und die überlagerte Reihe sonst zwischen ihnen hin und her springt. Lücken über vier Stunden trennen Fenster: über einen Metrik-Ausfall hinweg einen Takt zu mitteln, wäre eine erfundene Zahl.

**Belege, dass die Lesart trägt (wdqs, 30 Tage):**

- Serverseitig gerechnet ergibt `sum by (dag_id) (changes(...))` **397** Läufe, die Rekonstruktion aus den Rohsamples **398**.
- Mediane Laufzeit 3733,8 s, medianer Abstand zweier Laufenden 3720 s: 13,8 Sekunden Differenz, unter der Scrape-Auflösung von einer Minute. Der Abstand stammt aus den Zeitstempeln, die Dauer aus den Werten. Zwei unabhängige Größen, die sich gegenseitig prüfen, und sie passen nur zusammen, wenn `max_active_runs=1` die Läufe wirklich hintereinander legt.
- Damit ist auch die Einheit belegt: Millisekunden, nicht Sekunden.

**Grenze:** Zwei aufeinanderfolgende Läufe mit exakt gleicher Dauer auf die Millisekunde zählen als einer. Bei Fließkomma-Dauern ist das praktisch ausgeschlossen, und der Fehler ginge zu unseren Ungunsten (ein Lauf zu wenig). Bei zehn Wikimedia-DAGs meldet die Gauge **mehr** Wertwechsel, als der Takt erlaubt (`refine_api_requests_hourly`: 3360 in 30 Tagen bei stündlichem Takt). Die Ursache ist unbekannt, und für diese DAGs rechnen wir kein λ, statt eine Erklärung zu erfinden.

**Task-Ebene:** nicht möglich. Für den Spark-Task und den Abschluss-Task existiert keine Dauer-Metrik, `airflow_task_duration` trägt weder `dag_id` noch `task_id`, und die Sensoren melden im Reschedule-Modus Dauern nahe null. Gerechnet wird auf DAG-Ebene, und das steht im Report.

---

## ADR-019 — Der Wikimedia-Fall belegt die These, nicht das Werkzeug

**Status:** entschieden, 2026-07-14 (Abnahme Session 005). **Nummern-Hinweis:** Dieses ADR hieß bis zur Abnahme von 006 ebenfalls "ADR-017" und kollidierte mit der Gauge-Rekonstruktion oben — der Orchestrator hatte die Nummer vergeben, ohne zu prüfen, dass Session 005 sie schon belegt hatte. Umbenannt am 2026-07-14; ältere Log-Einträge und abgeschlossene Specs, die "ADR-017" im Sinn dieser These zitieren, meinen dieses ADR. Es steht in der Datei vor ADR-018, weil es früher entstand.
**Kontext:** Session 005 meldet "λ = 3598,4 s an einer echten Pipeline gemessen" und stellt den Fall als Validierung des Analyzers dar. Die Abnahme hat nachgerechnet, wie diese Zahl entsteht.

`wikimedia/case.py`, `lambda_of()` baut:

```python
Pipeline(durations={"dagrun": duration}, intra=[], cross=[CrossEdge("dagrun", "dagrun", 1)])
```

Ein Knoten, eine Selbst-Kante. Der Max-Plus-Eigenwert eines solchen Graphen ist **per Definition das Kantengewicht**. In den Messdaten steht das unverstellt:

```
dauer_s.mittel = 3598.4   →   lambda_s.mittel = 3598.4
dauer_s.median = 3733.8   →   lambda_s.median = 3733.8
dauer_s.p95    = 3778.8   →   lambda_s.p95    = 3778.8
```

λ ist in jeder Statistik identisch mit der eingesetzten Dauer. Kondensation, Karp und Howard sind auf diesem Graphen eine Identitätsfunktion. "λ = 3598,4 s" heißt: **die mittlere Laufdauer beträgt 3598,4 s.**

**Das ist kein Fehler der Session.** Wikimedias Prometheus liefert `airflow_dagrun_duration`, also Dauern auf **DAG-Ebene**, keine Task-Dauern. Ohne Task-Dauern lässt sich der Task-Graph nicht gewichten, und das DAG-Ebenen-Modell fällt zwangsläufig auf einen Knoten zusammen. Die Session hat das einzig Mögliche getan.

**Entscheidung:** Der Fall wird als Beleg der **These** geführt, nicht als Validierung des **Werkzeugs**. Zwei Dinge, die daraus folgen und die nicht verwischt werden dürfen:

1. **Die Formulierung "1,6 Sekunden Reserve" wird gestrichen.** Sie unterstellt eine knappe Marge und damit einen Zufall. Nach `math.md` Abschnitt 9 (Korrelation Verspätung/Laufzeit = −0,504) ist das Gegenteil der Fall: Das System ist rückgekoppelt und **pendelt sich genau dort ein**, wo die mittlere Dauer ≈ T ist. Eine mittlere Dauer 1,6 s unter dem Takt ist kein Balanceakt, sondern der Fixpunkt, den ein selbststabilisierendes System einnehmen muss. Die Session schreibt die Zirkularität in `math.md` selbst hin ("die gemessenen Dauern sind bereits das Ergebnis des eingeschwungenen Zustands"). Dann darf dieselbe Zahl nicht als knappe Marge verkauft werden.

2. **Für DAGs, deren einzige Cross-Run-Kante Signal G ist, gilt λ = Makespan.** Das ist die Laufzeit, die jedes Tool heute schon anzeigt. Der Analyzer verdient sein Geld erst dort, wo der Kreis ein **Teilpfad** ist (`depends_on_past` an einer einzelnen Task, `ExternalTaskSensor` mit `execution_delta`): dann ist λ < Makespan, und die naive Regel "Laufzeit gegen Schedule" liegt in beide Richtungen falsch. wdqs ist damit der **am wenigsten** aussagekräftige Falltyp für das Produkt.

**Die richtige Überschrift des Falls steht bereits in der Session, an anderer Stelle:** 30 DAGs laufen im Median länger als ihr Takt, 29 davon driften nicht, weil ihre Läufe überlappen dürfen. Das ist der Beleg, dass "Laufzeit über Takt" als Diagnose wertlos ist, und das ist an echten Daten gemessen. Diese Zahl trägt den Fall, nicht λ = 3598,4.

**Nebenbefund, der in den Report gehört:** λ auf dem Mittelwert ist ausreißer-empfindlich. Bei `wcqs` verzerrt ein einzelner hängender Lauf von 400.132 s (4,6 Tage) den Mittelwert um rund 560 s bei 712 Läufen. Für den asymptotischen Drift ist der Mittelwert die richtige Statistik, aber ein hängender Lauf vergiftet ihn. Das ist zu benennen, nicht zu glätten.

---

## ADR-018 — Zwei Risiko-Klassen: die Kern-Quote bleibt definitionsgleich, Signal G bekommt eine eigene

**Status:** entschieden, 2026-07-14 (Session 006)

**Kontext:** ADR-016 macht `max_active_runs=1` (Signal G) zum starken Signal. Würde G stillschweigend in das `STRONG`-Set von `report.py` wandern, spränge die Risiko-Quote nach oben — und zwar durch eine Definitionsänderung **nach** dem ersten Scan. Das ist genau die Bewegung, die ADR-005 verbietet, wenn sie die Zahl aufbläst: der erste kritische Leser sagt "eure Risiko-Kandidaten sind DAGs, deren Laufzeit über dem Takt liegt, das zeigt mir jedes Dashboard". Er hätte recht: für DAGs, deren einzige Cross-Run-Kante G ist, gilt λ = Makespan (ADR-017), Laufzeit-Monitoring gibt dort dieselbe Antwort. Gleichzeitig wäre Weglassen falsch — der Wikimedia-Fall hat gemessen, dass die Kante real bindet.

**Entscheidung:** Zwei getrennt ausgewiesene Klassen.

| Klasse | Definition | Bedeutung |
|---|---|---|
| `risk_candidate` (Kern) | mindestens ein starkes Signal aus **A, B, C, D, F** und sub-täglich | der Kreis ist ein Teilpfad, λ < Makespan möglich, kein heutiges Tool beantwortet das. Das bleibt die Launch-Zahl, und sie bleibt mit Session 003 vergleichbar, weil die Definition unverändert ist |
| `risk_candidate_g_only` | **nur** G als starkes Signal und sub-täglich | Kante real, aber λ = Makespan; beantwortbar durch Laufzeit-Monitoring. Eigene Zeile im Report, nie in die Kern-Quote gemischt |

Ein DAG mit A–F-Signal **und** G zählt in die Kern-Klasse; G wird als Spalte (`sig_g_max_active_runs`) trotzdem ausgewiesen.

**Begründung:** Die Zwei-Klassen-Lösung erlaubt beides zugleich: die Kern-Quote bleibt definitionsgleich und damit über die Scans hinweg vergleichbar, und die neue Kante steht daneben, mit gemessener Begründung statt versteckt. Der Report legt die Definitionsänderung offen, statt sie in eine gewachsene Zahl einzubacken. Für die Produkt-Aussage ist die Trennung ohnehin die ehrlichere: der Analyzer verdient sein Geld dort, wo der Kreis ein Teilpfad ist, nicht dort, wo ein Dashboard reicht.

**Umsetzung:** `report.py` — `STRONG` bleibt unverändert (A, B, C, D, F-stark), neue Menge für G, neue Spalten `sig_g_max_active_runs` und `risk_candidate_g_only` in `scan_results.csv`, eigene Zeile im Report. `scanner/analyze.py` bleibt unberührt: dessen `STRONG_KINDS` (mit G) speist nur die Lauf-Konsole, die Klassifikation für die Marktzahl passiert in `report.py`.

---

## ADR-020 — Signal F zählt für die Marktzahl, erzeugt aber keine λ-Kante

**Status:** entschieden, 2026-07-14 (Session 007)

**Kontext:** Signal F in der `*_success`-Variante ist seit ADR-011 ein starkes Signal und zählt in die Risiko-Kandidaten-Quote. Beim Übersetzen der Signale in Kanten (Spec 007) stellte sich die Frage, welche λ-Kante `{{ prev_start_date_success }}` erzeugt.

**Entscheidung:** Keine. Der Parser meldet F als Befund ("Datenabhängigkeit ohne Wartesemantik", Warnung `prev_run_success` bzw. `prev_run_date`), erzeugt aber keine Cross-Run-Kante.

**Begründung:** Das Template rendert einen Zeitstempel zur Laufzeit und **wartet nicht**. Ein Task mit `prev_start_date_success` startet pünktlich, auch wenn der Vorlauf noch läuft — er liest dann schlimmstenfalls veraltete Daten. Das ist ein Korrektheits-, kein Durchsatz-Problem: es gibt keine Scheduling-Abhängigkeit, die den Takt begrenzt, also keine Kante, die λ heben darf. Eine erfundene Kante würde λ fälschlich erhöhen und die Untergrenzen-Eigenschaft (math.md, Abschnitt 8) zwar behalten, aber die Schärfe der Zahl mit einer Behauptung erkaufen, die Airflows Semantik nicht hergibt.

**Die Divergenz ist gewollt und muss offen bleiben:** Marktzahl und λ-Modell messen zwei verschiedene Dinge. Die Marktzahl zählt Strukturen, in denen ein Lauf den vorigen *liest* — dafür ist F ein starkes Signal (ADR-011, unverändert). Das λ-Modell zählt Kanten, auf die ein Lauf *wartet* — dafür ist F keines. Wer die beiden Zahlen nebeneinander sieht, muss diese Erklärung finden können; sie gehört in den Report-Text (Session 009), sonst wirft uns die Differenz später jemand als Inkonsistenz vor.

**Umsetzung:** `eigenlag/parse_airflow.py` erzeugt für F Warnungen statt Kanten; die Konsistenz mit dem Scanner (der F weiterhin als Signal-Art zählt) sichert `scanner/parse_consistency_test.py` über die Abbildung Warnung → Signal-Art.

---

## ADR-021 — Ein Selbst-Referenz-Sensor ist die sauberste Cross-Run-Kante und wird modelliert

**Status:** entschieden, 2026-07-15 (Abnahme Session 008)
**Kontext:** Der Sensor-Nachlauf aus 008 hat einen Fall umklassifiziert: `bhatiadeepak0805/OmniRoute_Project_Group_4`, `DAG_Codes/dag_2.py:480`. Ein `ExternalTaskSensor` zeigt dort auf den **eigenen** DAG:

```python
wait_for_registry_00_00 = ExternalTaskSensor(
    task_id="wait_for_registry_00_00",
    external_dag_id="dag2_batch_pipeline_harsh",   # == der eigene DAG
    external_task_id="vehicle_registry_silver",
    execution_delta=timedelta(hours=5),            # == genau eine Periode (T = 5 h)
)
```

Der Parser aus 007 behandelt `external_dag_id` als Fremd-DAG-Verweis und meldete "Ziel nicht im Parse-Satz". Die Handprüfung der Session hat die Selbst-Referenz erkannt und die Entscheidung korrekt an den Orchestrator eskaliert.

**Entscheidung:** `ExternalTaskSensor` mit `external_dag_id == eigener dag_id` und `execution_delta = n × T` (n ganzzahlig ≥ 1) wird als Cross-Run-Kante modelliert: `CrossEdge(ziel_task, sensor_task, n)`, im selben Namespace, ohne Cross-DAG-Merge.

**Begründung:** Das ist semantisch die **sauberste** Sensor-Kante überhaupt: beide Enden liegen im selben DAG, das T-Gleichheits-Problem und das Merge-Problem aus Spec 007 entfallen vollständig. Lauf k wartet auf eine Task aus Lauf k−n — genau die Rekurrenz, die das Tool berechnet. Sie nicht zu modellieren wäre ein Falsch-Negativ ohne jeden technischen Grund; die 007-Beschränkungen (gleicher Takt, Ziel im Parse-Satz) sind hier per Konstruktion erfüllt.

**Umsetzung:** in Session 009 (Parser-Erweiterung plus Tests: n = 1, n = 2, delta kein Vielfaches → Warnung wie gehabt). Die 007-Korpus-Artefakte sind Engineering-Artefakte, keine Launch-Zahlen (die Scanner-Zählung von Signal C ist unabhängig davon) — sie werden bei Gelegenheit, nicht zwingend sofort, neu gerechnet.

**Umgesetzt am 2026-07-15 (Session 009).** Der 007-Graph-Check lief direkt danach neu: 4836/4836 Graphen Karp = Howard, einzige inhaltliche Änderung ist die jetzt modellierte OmniRoute-Kante (`DAG_Codes/dag_2.py:480`, `sensor_not_modeled` 27 → 26).

**Einordnung des Fundes, ehrlich:** Das Repo sieht nach Kursarbeit aus (Projektgruppen-Name). Der Wert liegt in der Semantik-Lücke, die es aufgedeckt hat, nicht im Fall als Launch-Material.
