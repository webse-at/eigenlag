# Session 009 — CLI `eigenlag analyze`: das Produkt in fremde Hände

**Phase 2, der Feedback-Meilenstein-Gate.** Nach dieser Session geht das Tool an echte Teams (Roadmap, Feedback-Meilenstein). Das heißt: Der Report wird von Leuten gelesen, die keine unserer Wiki-Seiten kennen, und jede Zahl, die er zeigt, muss sich selbst erklären und selbst begrenzen. Der Report **ist** das Produkt dieser Session, nicht die CLI-Mechanik.

## Vorher lesen

- `wiki/decisions.md`: ADR-002 (Kreis doppelt zeigen), ADR-007 (kein Kreis → nicht anwendbar), ADR-019 (These vs. Werkzeug), ADR-021 (Selbst-Referenz-Sensor, **in dieser Session umzusetzen**)
- `wiki/math.md` Abschnitte 7 (Stochastik), 8 (Grenzen), 9 (Uhr-Rückkopplung)
- `wiki/positioning.md`, Zwischenbewertung nach Phase 1 — der Report muss die dort beschriebene Erzählung tragen (Wissenslücke, Fehlalarm-Vermeidung), nicht die Angst-Erzählung
- `eigenlag/analyze.py`, `eigenlag/durations.py` (die Kette steht, die CLI ist eine Schale darum)

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **`argparse`, kein click/typer.** Regel 10. Entry-Point `eigenlag` über `[project.scripts]` in `pyproject.toml`.
2. **Monte Carlo in stdlib, `numpy` bleibt draußen — mit Messvorbehalt.** Die Lognormal-Fits kommen analytisch aus den vorhandenen Aggregaten: `mu = ln(p50)`, `sigma = (ln(p95) − ln(p50)) / 1.6449` (1.6449 = z-Wert der 95. Perzentile). Kein Schema-Umbau, keine Rohdaten nötig. Sampling per `random.lognormvariate`, Auswertung per `statistics.quantiles`. **Messvorbehalt:** Wenn 1000 Samples auf der Demo-Pipeline länger als 5 s brauchen, ist `numpy` erlaubt — erst messen, Zahl ins Log, dann entscheiden. Ein Zero-Dep-CLI via `pipx` ist ein echtes Argument beim Feedback-Meilenstein, das geben wir nicht kampflos her.
3. **Tasks ohne Varianz-Basis samplen deterministisch.** `assume`-Werte (n = 0) und Tasks mit n < 5 haben keine belastbare Streuung; sie gehen als Konstante ins Sampling, mit Warnung im Report. Eine erfundene Varianz wäre eine erfundene p95.
4. **Kondensation läuft pro Sample neu.** Die Kantengewichte der kondensierten Matrix sind längste Pfade und hängen von den gezogenen Dauern ab — bei anderen Dauern kann ein anderer Pfad der längste sein. Wer die Matrix einmal baut und nur λ neu rechnet, rechnet falsch. Ein Test pinnt das: eine Fixture, bei der zwei Intra-Pfade um den längsten konkurrieren, muss bei extremen Samples den Pfad wechseln.
5. **What-if-Syntax wie im ursprünglichen Auftrag:** `--what-if task=NAME:SEKUNDEN` und `--what-if drop-edge=SRC->DST`, wiederholbar. Zusätzlich rechnet der Report ungefragt das Standard-Ranking: jede Task auf dem kritischen Kreis halbiert, jede Cross-Kante entfernt, sortiert nach neuem λ (die Demo-Prototyp-Szenarien als Automatik).
6. **ADR-021 wird hier umgesetzt** (Selbst-Referenz-Sensor → `CrossEdge` im eigenen Namespace). Tests: n = 1, n = 2, delta kein ganzzahliges Vielfaches → Warnung wie gehabt. Danach den 007-Korpus-Graph-Check einmal neu laufen lassen (Karp = Howard, ~5 min), damit der Konsistenz-Beleg zum geänderten Parser passt.

## Auftrag

### 1. `eigenlag/cli.py`

```
eigenlag analyze PFAD
  [--db URL | --rest URL --rest-token TOKEN | --assume-duration SEK]
  [--dag-id ID]              nur diesen DAG (sonst: alle im Pfad)
  [--statistic mean|p50|p95] Default mean, im Report begründet
  [--since TAGE]             Fenster für die Metadaten-DB, Default 90
  [--period SEK]             Takt-Override, wenn Schedule unbekannt/dataset-getriggert
  [--samples N]              Monte Carlo, Default 1000; 0 schaltet ab
  [--what-if ...]            wiederholbar, siehe oben
  [--json]                   maschinenlesbar statt Text
```

Quellen-Mischung wie in 008: DB/REST liefert, was sie hat, `--assume-duration` füllt Lücken je Task mit Warnung. Ohne jede Quelle: Abbruch mit Erklärung, was fehlt (kein stiller Default).

Exit-Codes: 0 = analysiert (auch wenn instabil — das Urteil ist Sache des Nutzers, das Gate kommt in 010), 1 = Bedienfehler, 2 = Pfad geparst, aber kein analysierbarer DAG (mit Warnungs-Liste).

### 2. Der deutsche Report (`eigenlag/report.py`)

Aufbau, in dieser Reihenfolge — sie ist bewusst so herum (Urteil zuerst, Zahlen danach, Grenzen zum Schluss, aber pflichtig):

1. **Kopf:** DAG, Takt T (Quelle: Schedule-Ausdruck oder `--period`), Dauern-Quelle (DB mit Fenster / REST / angenommen), Stichproben-Basis (Läufe je Task min/median).
2. **Das Urteil:** λ gegen T. Drei Fälle: stabil (λ < T, mit Reserve in Prozent), **an der Grenze** (|λ−T| < 10 % von T — hier steht der `math.md`-Abschnitt-9-Hinweis, dass eingeschwungene Systeme genau hier landen), instabil (λ > T, mit Drift λ−T pro Lauf und der Zeit bis eine Stunde Rückstand erreicht ist). Kein Kreis → "nicht anwendbar: keine Cross-Run-Kante", ausdrücklich **nicht** "λ = 0" (ADR-007).
3. **Der kritische Kreis, doppelt** (ADR-002): kondensiert und als aufgelöster Task-Pfad, je Segment mit Datei:Zeile der erzeugenden Kante (die Herkunft steht seit 007 in `ParsedCrossEdge`).
4. **Monte Carlo:** λ_p50 und λ_p95 nebeneinander mit dem Satz, was p95 bedeutet ("hält der Takt auch in einer schlechten Woche"). Wenn λ_p95 > T > λ_p50: genau das aussprechen — "instabil in schlechten Wochen, erholt sich in guten; die Verspätung pendelt statt zu wachsen".
5. **What-if-Ranking:** Standard-Szenarien plus die angefragten, sortiert, mit dem Satz über Nicht-Kreis-Optimierungen ("bringt exakt null" — der Positioning-Kernsatz).
6. **Pflicht-Warnblock, nie abschaltbar:**
   - Sensor auf dem kritischen Kreis → der 008-Warntext (gemessene Dauer enthält Wartezeit, λ evtl. überschätzt, Abschnitt-9-Rückkopplung)
   - angenommene/dünne Dauern je Task
   - nicht modellierte Kanten (Sensor-Warnungen aus dem Parser) mit Datei:Zeile
   - **Modellgrenzen als Standard-Fußzeile:** unbegrenzte Parallelität angenommen (λ ist Untergrenze), Retries/Pools nicht modelliert (können real nur verlangsamern), Latenz-Angaben sind Makespan.

Sprache: ruhige, ganze Sätze, keine Emojis, keine Farb-Eskalation. Der Leser ist ein Data Engineer, der dem Tool noch nicht traut — jede Behauptung trägt ihre Grundlage bei sich. `--json` liefert dieselben Felder strukturiert (stabile Key-Namen, sie werden ab 010 vom CI-Gate gelesen).

### 3. Monte Carlo (`eigenlag/montecarlo.py`)

Pro Sample: Dauern je Task aus Lognormal(mu, sigma) ziehen (deterministische Tasks konstant), Kondensation + Howard neu, λ sammeln. Ausgabe λ_p50, λ_p95, Anteil Samples mit λ > T. Seed-Parameter für Reproduzierbarkeit, Default fest (derselbe Aufruf liefert dieselben Zahlen — ein Report, der bei jedem Lauf andere Werte zeigt, verspielt Vertrauen). Perf-Messung 1000 Samples auf der Demo-Pipeline ins Log (Vorentscheidung 2).

### 4. Verifikation (Kern-Akzeptanz, drei echte Läufe)

1. **Demo-Pipeline als CLI-Fixture:** die Prototyp-Demo als DAG-File nachgebaut liegt vermutlich noch nicht vor — stattdessen: `analyze` als Python-Aufruf gegen die bekannte Demo-`Pipeline` UND die CLI gegen ein minimales DAG-File mit `--assume-duration`. λ = 4.40 h-Pfad bleibt über die bestehenden Kern-Tests gepinnt.
2. **Gegen echtes Airflow:** die 008-venv liegt noch (`.venv-airflow/`), die Test-DB von damals nicht mehr. Läufe reproduzieren, diesmal `AIRFLOW_HOME` unter `data/airflow-home/` (bleibt liegen, `.gitignore`), dann: `eigenlag analyze <test-dags> --db sqlite:///data/airflow-home/airflow.db` — vollständigen Report ins Log pasten. Das ist der Report, den ein echtes Team sehen würde.
3. **Postgres-Wegwerf-Container** (offener Punkt aus 008): `docker run --rm postgres:16` (Docker läuft, geprüft bei der Spec-Erstellung), Fixture-Zeilen laden, `from_metadata_db` gegen Postgres — `percentile_cont`-Pfad muss dieselben Zahlen liefern wie der SQLite/Python-Pfad auf denselben Daten. Container danach weg, nichts persistiert.
4. **Flaggschiff:** `eigenlag analyze` auf `load_data_wikiviews` mit `--assume-duration 300` — der Report, der später in Launch-Material zitiert wird. Ins Log.

### 5. `pipx`-Probe (vorgezogen aus 011, nur als Rauchtest)

`pipx` installieren (fehlt auf dem Server), `pipx install .`, `eigenlag --help` und ein `analyze`-Aufruf. Nicht das volle Packaging (README, Versionierung bleiben 011) — nur der Beweis, dass die Entry-Point-Mechanik trägt, solange wir noch keine Nutzer haben, die es zuerst merken.

## Akzeptanz

- CLI installiert und läuft via `pipx` (Rauchtest), `eigenlag --help` gezeigt
- Die vier Verifikations-Läufe im Log, mit vollständigen Reports (nicht Ausschnitten)
- ADR-021 umgesetzt inkl. der drei Tests; 007-Graph-Check neu gelaufen und grün
- Postgres = SQLite auf denselben Fixture-Daten, Output im Log
- Monte-Carlo-Pfadwechsel-Test grün (Vorentscheidung 4); Perf-Zahl im Log; Seed-Determinismus getestet
- Kein-Kreis-Fall im Report als "nicht anwendbar" formuliert, per Test gepinnt
- `--json`-Ausgabe schema-stabil, ein Test liest sie zurück
- `pytest`, `ruff`, `mypy` grün; Pflicht-Dependencies unverändert null (oder `numpy` mit Messbeleg > 5 s)

## Explizit nicht in dieser Session

CI-Gate und Exit-Code-Semantik für Diffs (010), README/Packaging-Polish (011), dbt (nach Feedback-Meilenstein), Web-Anything. Und keine Report-Ausgabe auf Englisch — deutsch zuerst, i18n ist eine Frage für nach dem Feedback.
