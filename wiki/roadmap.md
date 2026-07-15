# Roadmap

Der Session-Plan. Jede Zeile ist genau eine CC-Session mit genau einer Spec in `cc-sessions/`. Der Plan ist zweimal von der Wirklichkeit korrigiert worden (005 wurde der Wikimedia-Fall statt des Parsers, 006 wurde der Re-Scan) — die Tabelle zeigt den Ist-Stand, nicht den ursprünglichen Entwurf.

## Phase 1 — Scanner v2 (Marktbeweis, Launch-Content)

| Session | Inhalt | Stand |
|---|---|---|
| 001 | Harvest: GitHub Code-Search, Kandidaten, Filter, Resume | ✅ done, 1692 Kandidaten |
| 002 | AST-Analyse: DAG-Scoping, Signale, Schedule-Klassifikation, dbt | ✅ done, 141 Tests |
| 003 | Scan-Lauf über 1692 Repos, CSVs, Report, Stichproben | ✅ done — Zahlen durch ADR-015/016 veraltet |
| 005 | Wikimedia-Fall: λ an einer Produktions-Pipeline, PromQL-Messung | ✅ done — Einordnung per ADR-019 korrigiert |
| 006 | **Re-Scan** mit Konstruktoren (ADR-015) und Signal G (ADR-016) als eigener Klasse (ADR-018), Vorher/Nachher-Tabelle, Fall-Korrektur in `case.md` | ✅ done — Kern-Quote unverändert 176, dazu 473 G-only; `scan/v2/` ist der zitierfähige Stand |

**Akzeptanz Phase 1 (nachgeschärft):** Die Launch-Zahlen stammen aus `scan/v2/`, jede mit Nenner und Permalink-Beleg, die Definitionsänderung offengelegt. Erst danach ist irgendetwas öffentlich behauptbar.

## Phase 2 — Analyzer-Core als CLI `eigenlag`

Nummern verschoben, weil 005 und 006 dazwischenkamen. Inhalte unverändert.

| Session | Inhalt | Abhängig von |
|---|---|---|
| 004 | Mathe-Kern: Kondensation, Karp, Howard, Drift, `periods` | ✅ done, abgenommen (Brute-Force-Kreuzvergleich) |
| 007 | Airflow-Parser per AST: Tasks, `>>`/`<<`, `set_upstream/downstream`, Signale inkl. G, Schedule. Wiederholung des Karp/Howard/Brute-Force-Vergleichs auf echten geparsten DAGs (offen aus Abnahme 004) | ✅ done — Karp = Howard auf 4836 Korpus-Graphen, 129 Teilpfad-Fälle gefunden (ADR-019-Auflage erfüllt), ADR-020 |
| 008 | Dauern-Schicht (Metadaten-DB, REST, `--assume-duration`), `analyze()`, Schema-Verifikation gegen echtes Airflow, Sensor-Nachlauf aus 007a. Der dbt-Parser wurde beim Spec-Schnitt herausgenommen (eigene Zeile unten) | ✅ done — erstes λ in Sekunden end-to-end; Airflow 3.3.0: `/api/v1` und Basic Auth entfernt, v2+JWT umgesetzt; Sensor-Nachlauf 1/11/2, kein `periods > 1` in freier Wildbahn |
| 008b | dbt-Parser (`manifest.json`) — **vertagt bis nach dem Feedback-Meilenstein** (Entscheidung in Abnahme 008a): dbt-λ ist strukturell fast immer der λ=1-Fall, der Produktwert dort ist die Abdeckungs-Erzählung, und das Feedback echter Teams soll erst zeigen, welchen Report dbt-Nutzer brauchen | Feedback-Meilenstein |
| 009 | **Erledigt (2026-07-15).** CLI `eigenlag analyze` (Airflow-only), deutscher Report (sagt, was `simulate` misst: Makespan), What-if-Ranking, Monte Carlo (λ_p50, λ_p95 — stdlib, 1000 Samples in 0,05 s, `numpy` blieb draußen). Dazu: ADR-021 im Parser (Selbst-Referenz-Sensor), Postgres-Aggregationspfad gegen Wegwerf-Container verifiziert | 007, 008 |
| 010 | CI-Gate `eigenlag check --against main`, Exit-Code, PR-Kommentar-Text | 009 |
| 011 | Packaging, `pipx install .` verifiziert, README mit dem Bäckerei-Beispiel | 010 |

**Akzeptanz Phase 2:** `pipx install .` läuft, `eigenlag analyze <pfad> --db <url>` liefert λ, kritischen Kreis und What-if-Ranking, CI-Gate-Test grün.

**Feedback-Meilenstein (umgeplant 2026-07-15):** Die ursprüngliche Idee "an 2–3 Teams aus dem Netzwerk" scheitert daran, dass es dieses Netzwerk nicht gibt — David kennt keine Data-Engineering-Teams. Die Alternative ist der öffentliche Weg, und er passt zur ohnehin geplanten Solo-Founder-Identität (Reddit/X, Dev-Crowd):

1. **010 und 011 zuerst fertig** (Gate plus Packaging/README) — veröffentlicht wird ein installierbares Tool, kein Rohbau.
2. **Repo öffentlich stellen** und ein Content-Stück, das Wert liefert statt zu werben: der Wikimedia-Sweep (30 DAGs über Takt, 29 driften nicht — "Laufzeit über Takt ist als Diagnose wertlos") als Post in r/dataengineering, dazu Airflow-Community-Slack. Die Zahlen aus `scan/v2/` und der Fall aus `wikimedia/case.md` sind genau dafür gebaut worden: jede mit Permalink belegbar.
3. **Wikimedia direkt anschreiben.** Wir haben deren Pipelines vermessen, die Daten sind öffentlich, der Befund ist für sie interessant (Phabricator/Mailing-Liste). Das ist die eine "echtes Team"-Rückmeldung, die ohne Netzwerk erreichbar ist.

Konsequenz für 011: Die Zielgruppe liest englisch. README englisch, und die Report-Sprachfrage (EN-Fassung oder `--lang`) wird in der 011-Spec entschieden — `compose()`/`render()` aus 009 hat die Trennung dafür schon.

Jede nach außen gerichtete Aktion (Repo public, Posts, Anschreiben) löst David selbst aus; die Sessions bereiten nur Material vor.

## Reihenfolge

006 vor allem anderen: solange die Korpus-Zahlen veraltet sind, gibt es keinen behauptbaren Marktbeweis, und der Wikimedia-Fall ist ohne die korrigierte Darstellung nicht zitierfähig. Danach Phase 2 ab 007.

Die Lehre aus dem Wikimedia-Fall für Phase 2 (ADR-019): Der Analyzer beweist seinen Wert dort, wo der Kreis ein **Teilpfad** ist und λ < Makespan gilt. Der Parser (007) sollte deshalb früh einen echten Fall dieser Sorte finden und durchrechnen — das ist der Fall, der das Produkt trägt, nicht der G-only-Fall.

## Bewusst nicht gebaut

- **Web-UI oder Dashboard.** Das Tool ist ein CLI und ein CI-Gate. Ein Dashboard ist ein anderes Produkt.
- **Live-Monitoring.** Kein Daemon, kein Agent im Cluster. Der Analyzer läuft auf Anforderung.
- **Scheduler-Optimierung.** Das Tool sagt, wo der Engpass ist. Es räumt ihn nicht weg.
- **Dagster- und Prefect-Parser.** Erst wenn Airflow und dbt stehen und jemand danach fragt.
- **Interprozedurale Factory-Auflösung** (ADR-009) und **transitive Konstruktoren** (ADR-015): beides bewusst begrenzt, die Grenzen sind als Untergrenzen im Report ausgewiesen.
