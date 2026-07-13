# Session 003 — Scanner: Lauf, CSV, Report

**Phase 1, Schritt 3 von 3.** Hier entsteht der Marktbeweis. Die Zahlen aus dieser Session werden öffentlich behauptet, also ist die Stichprobe Pflicht, nicht Kür.

## Vorher lesen

- `wiki/signals.md`, `wiki/architecture.md` (Abschnitt "Was der Scanner nicht ist")
- `CLAUDE.md`, Abschnitt "Verifikation"

## Auftrag

### 1. Lauf

Orchestriere Harvest (001), Clone und Analyse (002) über die volle Kandidatenmenge: mindestens 200 Airflow-Repos, mindestens 100 dbt-Repos. Resume-fähig: ein Abbruch bei Repo 180 darf beim Neustart nicht 180 Clones neu ziehen.

Fortschritt auf stdout, damit ein Lauf über eine Stunde beobachtbar bleibt.

### 2. `scan_results.csv`

**Eine Zeile pro DAG**, nicht pro File und nicht pro Repo.

| Spalte | Inhalt |
|---|---|
| `repo` | `org/name` |
| `repo_url` | Link |
| `file` | Pfad im Repo |
| `dag_id` | wie im Code |
| `dag_lineno` | Zeile der DAG-Instanziierung |
| `schedule_raw` | der Ausdruck, wie er im Code steht |
| `schedule_class` | subdaily / daily_or_slower / none / dataset_triggered / unknown |
| `task_count` | Anzahl erkannter Tasks |
| `sig_a_depends_on_past` | 0/1 |
| `sig_b_wait_for_downstream` | 0/1 |
| `sig_c_ext_sensor_delta` | 0/1 |
| `sig_d_include_prior_dates` | 0/1 |
| `sig_e_dbt_incremental` | 0/1 |
| `sig_f_prev_success_tmpl` | 0/1 (nur die starken `*_success`-Varianten) |
| `sig_f_weak_prev_ds` | 0/1 (schwach, zählt **nicht** in die Risiko-Quote, siehe ADR-005) |
| `has_crossrun` | 1, wenn eines von A-F stark gesetzt ist |
| `risk_candidate` | 1, wenn `has_crossrun` **und** `schedule_class == subdaily` |
| `evidence` | `datei:zeile` je gesetztem Signal, semikolon-getrennt |
| `permalink` | `https://github.com/<repo>/blob/<sha>/<file>#L<lineno>` |

Der `permalink` ist die wichtigste Spalte. Er nutzt den **Commit-SHA** des Clones, nicht den Branch-Namen, damit der Link auch dann noch stimmt, wenn das Repo sich weiterentwickelt. Ohne festen SHA verrottet der Beleg, und ein verrotteter Beleg ist kein Beleg.

### 3. `report.md`

Kernzahlen, jede mit Nenner. Kein Prozentwert ohne die absolute Zahl daneben.

- Repos gescannt, Repos erfolgreich analysiert, Repos mit Fehler (mit Fehler-Kategorien)
- DAGs gefunden
- Anteil DAGs mit Cross-Run-Kante
- Anteil DAGs mit sub-täglichem Schedule
- **Anteil Risiko-Kandidaten** (Cross-Run **und** sub-täglich): das ist die Launch-Zahl
- Verteilung der Signale A bis F einzeln
- Top-10-Beispiele mit Repo-Link, DAG-Name, Schedule, gesetzten Signalen

Dazu ein Abschnitt **"Was diese Zahlen nicht sagen"**, verpflichtend, mit mindestens diesen Punkten:

- Die Stichprobe stammt aus der GitHub-Code-Search und ist nicht repräsentativ für alle Airflow-Nutzer. Sie ist eine Stichprobe über öffentliche Repos, die bestimmte Begriffe enthalten.
- Öffentliche Repos sind nicht Produktions-Pipelines. Der Anteil von Spielzeug-Code ist unbekannt, trotz Blocklist.
- Der Scanner sagt **nicht**, dass diese Pipelines instabil sind. Er sagt, dass sie die Struktur haben, in der Instabilität entstehen kann, und dass kein Tool ihnen zeigt, ob sie es sind. Der Unterschied ist nicht Kosmetik, er ist der Unterschied zwischen einer haltbaren und einer widerlegbaren Behauptung.

Wer diesen Abschnitt weglässt oder verwässert, hat die Session nicht bestanden.

### 4. Stichprobe (Akzeptanz-kritisch)

Ziehe **10 Treffer** mit `risk_candidate == 1`, gestreut über verschiedene Repos. Für jeden:

1. Permalink öffnen (per `curl` auf die raw-URL reicht, es geht um den Beleg, nicht um den Browser).
2. Prüfen: steht das Signal wirklich dort, in der angegebenen Zeile, im angegebenen DAG?
3. Prüfen: ist der Schedule wirklich sub-täglich?
4. Ergebnis in `sample_verification.md`: Repo, Datei, Zeile, was erwartet wurde, was tatsächlich dort steht, Urteil bestätigt oder False Positive.

**Wenn auch nur ein einziger False Positive auftaucht: Ursache finden, Analyse korrigieren, Lauf wiederholen.** Nicht die Stichprobe austauschen, bis sie passt. Eine False-Positive-Quote, die durch Nachziehen der Stichprobe entstanden ist, ist Betrug an der eigenen Zahl.

## Akzeptanz

- Lauf über >= 200 Airflow-Repos ohne Absturz. Beleg: Lauf-Output im Session-Log, mit Repo-Zähler und Laufzeit.
- `scan_results.csv`, `report.md`, `sample_verification.md` liegen vor.
- Alle 10 Stichproben bestätigt, oder Korrektur plus erneuter Lauf dokumentiert.
- `scan_errors.jsonl` existiert und wird im Report ausgewertet. Ein leeres Fehler-File bei 300 Repos ist verdächtig, nicht beruhigend, und wäre zu erklären.

## Danach

Orchestrator prüft gegen die Spec und entscheidet mit David, ob Phase 2 startet. Falls die Risiko-Kandidaten-Quote sehr niedrig ausfällt, ist das ein Ergebnis und kein Misserfolg. Es wäre dann offen zu besprechen, ob das Produkt trotzdem gebaut wird.
