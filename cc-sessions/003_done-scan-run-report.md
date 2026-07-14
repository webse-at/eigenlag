# Session 003 — Scanner: Lauf, CSV, Report

**Phase 1, Schritt 3 von 3.** Hier entsteht der Marktbeweis. Diese Zahlen werden öffentlich behauptet. Jede einzelne muss einen Nenner haben und jeder Treffer einen Beleg, den ein Fremder in dreißig Sekunden nachschlägt.

**Diese Spec wurde nach der Abnahme von 001 und 002 überarbeitet.** Die Befunde aus beiden Sessions stehen hier drin, insbesondere der Umgang mit dbt, mit Task-Factories und mit einer möglicherweise kleinen Risiko-Quote.

## Vorher lesen

- `wiki/signals.md` (Signal-Definitionen, Risiko-Definition)
- `wiki/architecture.md`, Abschnitt "Was der Scanner nicht ist"
- `wiki/decisions.md`, ADR-005, ADR-009, ADR-011
- `STATUS.md`, Abschnitt "Was der Orchestrator prüfen soll"

## Auftrag

### 1. Lauf

Clone und Analyse über die volle Kandidatenmenge aus `data/candidates.jsonl` (1692 Repos: 1328 Airflow, 364 dbt). Resume-fähig, Fortschritt auf stdout, Fehler nach `scan_errors.jsonl`, Lauf bricht nicht ab.

Der Rauchtest aus 002 lief über die **ersten** 40 Zeilen der Kandidatenliste. Die sind alle Treffer derselben Query und damit nicht repräsentativ. Der volle Lauf hat dieses Problem nicht, aber falls du zwischendurch eine Teilmenge testest: **zufällig ziehen, nicht die ersten n.**

### 2. Airflow und dbt sind zwei getrennte Auswertungen

**Das ist der wichtigste Punkt dieser Spec.** Ein dbt-Repo enthält Models, aber **keinen Schedule**. Wie oft ein Model läuft, steht in Airflow, in dbt Cloud oder in einem Cron außerhalb des Repos. Die Risiko-Definition lautet "starkes Signal **und** sub-täglich im selben DAG". Ein dbt-Repo kann diese Bedingung konstruktionsbedingt **nie** erfüllen.

Daraus folgt zwingend:

- Die **Risiko-Quote wird ausschließlich über Airflow-DAGs gebildet.** Nenner sind die gefundenen DAGs, nicht die Repos und nicht die dbt-Models.
- dbt bekommt eine **eigene Tabelle** mit einer eigenen Aussage: wie viele Models sind inkrementell mit `is_incremental()`, also mit echter Selbst-Kante. Das ist eine Aussage über Rekurrenz, nicht über Risiko.
- Wer dbt-Models in den Airflow-Nenner mischt, verdünnt die Quote und produziert eine Zahl, die nichts bedeutet. Wer sie in den Zähler mischt, behauptet ein Risiko, das er nicht belegen kann.

Der Satz für den Report: bei dbt kennen wir den Kreis, aber nicht den Takt. Genau deshalb ist ein Werkzeug nötig, das beides zusammenbringt.

### 3. `scan_results.csv`

**Eine Zeile pro DAG.** Nicht pro File, nicht pro Repo.

| Spalte | Inhalt |
|---|---|
| `repo` | `org/name` |
| `file` | **vollständiger** Pfad im Repo, nie gekürzt |
| `dag_id` | wie im Code |
| `dag_lineno` | Zeile der DAG-Instanziierung |
| `schedule_raw` | der Ausdruck, wie er im Code steht |
| `schedule_class` | subdaily / daily_or_slower / none / dataset_triggered / unknown |
| `task_count` | Anzahl erkannter Tasks |
| `sig_a_depends_on_past` | 0/1 |
| `sig_b_wait_for_downstream` | 0/1 |
| `sig_c_ext_sensor_delta` | 0/1 |
| `sig_d_include_prior_dates` | 0/1 |
| `sig_f_prev_success_tmpl` | 0/1, stark (ADR-011) |
| `sig_f_weak_prev_ds` | 0/1, schwach, **nicht** in der Quote (ADR-005) |
| `has_crossrun` | 1, wenn eines der starken Signale gesetzt ist |
| `risk_candidate` | 1, wenn `has_crossrun` **und** `schedule_class == subdaily` |
| `evidence` | `vollständiger/pfad.py:zeile` je Signal, semikolon-getrennt |
| `permalink` | `https://github.com/<repo>/blob/<sha>/<file>#L<lineno>`, mit dem **Commit-SHA** des Clones |

Der `permalink` ist die wichtigste Spalte. Mit Branch-Namen statt SHA verrottet der Beleg, sobald sich das Repo weiterentwickelt, und ein verrotteter Beleg ist kein Beleg.

**Zur Pfad-Vollständigkeit:** Bei der Abnahme von 001 stand im Log `dags/tutorial.py:35`, echt war `docker/sandbox/ubuntu-airflow/airflow/dags/tutorial.py`. Der Beleg ließ sich nicht auflösen. Im CSV ist das kein Schönheitsfehler, sondern macht die Zeile wertlos (Regel 6).

### 4. Zwei weitere CSVs

Weil sie sich keinem DAG zuordnen lassen, aber gezählt gehören:

- **`scan_factories.csv`** (ADR-009): Repo, vollständiger Pfad, Zeile, Funktionsname, welches Signal. Diese Treffer sind **echte** Signale an einem Ort, den die DAG-scoped Analyse nicht sieht. Sie fließen **nicht** in die Risiko-Quote, weil man sie ohne interprozedurale Analyse keinem DAG zuordnen kann.
- **`scan_dbt.csv`**: Repo, Model-Pfad, Zeile von `is_incremental()`, Materialisierung, woher sie kam.

### 5. `report.md`

Jede Zahl mit Nenner. Kein Prozentwert ohne die absolute Zahl daneben.

**Airflow-Block:**
- Repos gescannt, erfolgreich analysiert, fehlerhaft (mit Fehler-Kategorien aus `scan_errors.jsonl`)
- DAGs gefunden
- DAGs mit Cross-Run-Kante (absolut und Anteil)
- DAGs mit sub-täglichem Schedule (absolut und Anteil). Im Rauchtest waren das 17 Prozent, das ist mehr als erwartet und verdient einen Satz.
- **Risiko-Kandidaten: die Launch-Zahl.** Cross-Run **und** sub-täglich, im selben DAG.
- Signale A bis F einzeln
- Top-10-Beispiele mit Permalink, DAG-Name, Schedule, gesetzten Signalen

**dbt-Block:** getrennt, mit eigener Aussage (siehe Abschnitt 2).

**Untergrenzen-Block.** Unsere Quote ist zu klein, nicht zu groß, und zwar nachweislich an drei Stellen. Alle drei mit Zahl:
- **Task-Factories** (ADR-009): Signale, die wir sehen, aber keinem DAG zuordnen können.
- **`unresolved_default_args`**: `default_args` aus Import oder dynamischer Konstruktion, nicht auflösbar. Im Rauchtest 8 Fälle auf 40 Repos.
- **Ambige Tasks**: Operator ohne DAG-Zuordnung in einem File mit mehreren DAGs. Wird nicht geraten (Spec 002).

### 6. Abschnitt "Was diese Zahlen nicht sagen" (verpflichtend)

Wer ihn weglässt oder verwässert, hat die Session nicht bestanden. Mindestens:

- **Der 1000er-Deckel.** Vier der sechs Queries laufen hinein, `depends_on_past` meldet `total_count` 2284, geholt wurden 1000. Die Stichprobe ist nach oben abgeschnitten.
- **Die Stichprobe ist keine Zufallsauswahl** aus "allen Airflow-Nutzern", sondern aus öffentlichen Repos, die bestimmte Begriffe enthalten und über die Code-Search auffindbar sind.
- **Öffentliche Repos sind nicht Produktions-Pipelines.** Der Anteil Spielzeug-Code ist trotz Blocklist unbekannt.
- **`fork` und `archived` haben null Mal gegriffen** (nachgeprüft an 20 Kandidaten). Die klassische Code-Search liefert diese Repos offenbar nicht aus. Kein Filter-Fehler, aber ohne Erklärung liest es sich wie einer.
- **Die Blocklist verwirft 12 Prozent** (251 von 2095). Anfechtbar über `rejected.jsonl`, jede Zeile mit Grund.
- **Der Scanner sagt nicht, dass diese Pipelines instabil sind.** Er sagt, dass sie die Struktur haben, in der Instabilität entstehen kann, und dass kein Werkzeug ihnen zeigt, ob sie es sind. Das ist der Unterschied zwischen einer haltbaren und einer widerlegbaren Behauptung.

### 7. Wenn die Risiko-Quote klein bleibt

Der Rauchtest lieferte 4 von 352 DAGs, also gut ein Prozent. Ob das an der Realität liegt oder an der schiefen Stichprobe, entscheidet dieser Lauf.

**Falls die Quote klein bleibt, wird die Definition NICHT gelockert.** Nicht `prev_ds` doch mitzählen, nicht Factories in den Zähler holen, nicht "sub-täglich" auf "täglich oder schneller" ausdehnen. Jede dieser Bewegungen produziert eine größere Zahl, die beim ersten kritischen Leser kippt und alles andere mit entwertet (ADR-005).

Stattdessen wird die Aussage umformuliert, und zwar auf die, die ohnehin die stärkere ist:

> Wir haben N Pipelines mit einem Kreis über die Zeitachse gefunden. Für keine einzige davon ist bekannt, wo ihre Taktgrenze liegt, weil kein Werkzeug sie ausrechnet.

Diese Aussage hängt nicht an der Prozentzahl. Sie ist wahr, egal wie die Quote ausfällt, und sie verkauft keine Angst, sondern eine Wissenslücke. Der Report soll beide Zahlen zeigen: die Risiko-Quote und die Anzahl der Pipelines mit Kreis. Welche davon die Überschrift wird, entscheiden David und der Orchestrator nach dem Lauf, nicht die Implementer-Session.

### 8. Stichprobe (Akzeptanz-kritisch, beide Richtungen)

Die bisherige Spec prüfte nur Falsch-Positive. Nach 001 und 002 ist das **die kleinere Gefahr**: Der AST-Scanner ist streng, und die Factory-Entdeckung hat gezeigt, dass er eher zu wenig findet als zu viel. Deshalb jetzt zwei Stichproben.

**8a. Falsch-Positive.** Ziehe **10 Zeilen mit `risk_candidate == 1`**, zufällig, über verschiedene Repos gestreut. Je Zeile: Permalink auflösen, prüfen, ob das Signal wirklich dort steht, im richtigen DAG, und ob der Schedule wirklich sub-täglich ist.

Falls es **weniger als 10** Risiko-Kandidaten gibt: alle prüfen, und die Zahl im Report nennen. Nicht die Stichprobe mit `has_crossrun`-Zeilen auffüllen und so tun, als wären es zehn.

**8b. Falsch-Negative (neu).** Ziehe **10 Repos, die der Scanner als signalfrei meldet**, obwohl die Code-Search sie als Kandidat gefunden hat. Schau sie von Hand an: Warum kein Signal? Legitime Gründe sind `depends_on_past=False`, auskommentierter Code, Doku-Snippet. Kein legitimer Grund ist: ein Muster, das der Scanner nicht kennt.

Genau so wurde ADR-009 gefunden. Wenn hier ein zweites Muster auftaucht, ist das der wertvollste Fund der Session, und die Regel gehört in `analyze.py`, nicht in eine Fußnote.

Beide Stichproben nach `sample_verification.md`, mit Repo, vollständigem Pfad, Zeile, Erwartung, tatsächlichem Inhalt und Urteil.

**Wenn ein Falsch-Positiv auftaucht: Ursache finden, Analyse korrigieren, Lauf wiederholen.** Nicht die Stichprobe nachziehen, bis sie passt.

## Akzeptanz

- Voller Lauf über die Kandidatenliste ohne Absturz. Beleg: Lauf-Output im Log, mit Repo-Zähler und Laufzeit.
- `scan_results.csv`, `scan_factories.csv`, `scan_dbt.csv`, `report.md`, `sample_verification.md` liegen vor.
- Airflow- und dbt-Zahlen sind **getrennt**, kein gemeinsamer Nenner.
- Beide Stichproben durchgeführt und dokumentiert.
- `scan_errors.jsonl` im Report ausgewertet. Ein leeres Fehler-File bei 1692 Repos wäre verdächtig, nicht beruhigend.
- Tests, `ruff`, `mypy` grün.

## Danach

Der Orchestrator prüft gegen die Spec und entscheidet mit David, ob und wie Phase 2 weitergeht. **Eine kleine Risiko-Quote ist ein Ergebnis, kein Misserfolg.** Sie zu vergrößern, indem man die Definition dehnt, wäre einer.
