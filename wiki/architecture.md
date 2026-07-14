# Architektur

## Zwei Produkte, ein Kern

Das Projekt liefert zwei Dinge, die sich dieselbe Signal-Definition teilen:

1. **Scanner** (Phase 1): läuft einmal über öffentliche Repos, produziert Marktzahlen und Launch-Content. Wegwerf-Code im besten Sinn, aber mit belegbaren Ergebnissen.
2. **Analyzer** (Phase 2): das eigentliche Produkt. Ein CLI namens `eigenlag`, das auf einer echten Pipeline λ, kritischen Kreis und What-if-Ranking berechnet.

Beide benutzen dieselbe Definition der Cross-Run-Signale aus [signals.md](signals.md). Wenn sie auseinanderlaufen, widerspricht das Produkt der eigenen Marktbehauptung.

## Zielstruktur

```
eigenlag/
├── CLAUDE.md
├── STATUS.md
├── README.md
├── pyproject.toml
├── wiki/
│   └── maxplus_pipeline.py      Referenz-Prototyp, unverändert, Fixture
├── cc-sessions/                 Session-Specs
├── scanner/                     Phase 1, eigenständig
│   ├── harvest.py               GitHub Code-Search, Repo-Kandidaten, Filter
│   ├── clone.py                 Shallow-Clones mit Disk-Cache
│   ├── wrappers.py              DAG-Konstruktoren, die das Repo selbst definiert (ADR-015)
│   ├── analyze.py               AST-Analyse je Repo, DAG-scoped, plus Factories
│   ├── analyze_dbt.py           Signal E: incremental UND is_incremental()
│   ├── report.py                CSV plus report.md
│   ├── parse_corpus.py          Korpus-Validierung des Parsers (Session 007), Artefakte in scan/007_parse/
│   ├── parse_consistency_test.py  Parser und Scanner müssen je DAG dieselben Signal-Arten finden
│   ├── fixtures/                Nachgebaute Repos als Testdaten, kein Code (aus Lint/Typing raus)
│   └── *_test.py
├── wikimedia/                   Der erste echte Fall, eigenständig (Session 005)
│   ├── fetch.py                 Prometheus über Wikimedias Grafana-Proxy, Cache-first
│   ├── runs.py                  Läufe aus der Gauge rekonstruieren (ADR-017)
│   ├── case.py                  Scan, Messung, λ über den Kern, Sweep über die Organisation
│   ├── case.md                  Der Fall, mit PromQL und Permalink zu jeder Zahl
│   └── wikimedia_dags.csv       453 Zeilen, je DAG und Airflow-Instanz
└── eigenlag/                    Phase 2, das Package
    ├── model.py                 Task, Edge, Pipeline, CrossEdge (Datentypen)
    ├── maxplus.py               Kondensation, Karp, Howard, Drift
    ├── schedule.py              Cron, Preset, timedelta, Dataset: sub-täglich oder nicht,
    │                            plus period_seconds: der Takt T als Zahl (aus scanner/ umgezogen, 007)
    ├── parse_airflow.py         AST-Parser: DAG-Files → ParsedDag; to_pipeline heiratet Struktur mit Dauern
    ├── montecarlo.py            Lognormal-Fits, λ_p50, λ_p95
    ├── whatif.py                What-if-Szenarien und Ranking
    ├── parse_dbt.py             manifest.json
    ├── durations.py             Airflow-Metadaten-DB, REST, --assume-duration
    ├── gate.py                  CI-Gate, λ vor/nach Diff
    ├── report.py                Nutzer-Ausgabe (deutsch)
    ├── cli.py                   Argument-Parsing, Subcommands
    └── *_test.py
```

Die Abhängigkeits-Richtung ist Produkt ← Scanner: der Scanner importiert aus dem Package (`eigenlag.schedule`, in Tests auch `eigenlag.parse_airflow`), nie umgekehrt. Der Scanner ist Wegwerf-Code, das Package nicht.

Tests liegen neben dem Source-File, nicht in einer Sammelhalde. `karp.py` und `karp_test.py` sind Nachbarn.

## Datenfluss Analyzer

```
DAG-Files (Airflow)  ─┐
manifest.json (dbt)  ─┼─→  Parser  ─→  Pipeline (Tasks, Intra-Kanten, Cross-Run-Kanten)
                      │                      │
Metadaten-DB / REST  ─┘                      │  Dauern p50/p95 je Task
                                             ▼
                                       Kondensation
                                             │  Abar[ziel][quelle]
                                             ▼
                                   Howard  ──┴──  Karp (Kontrolle)
                                             │
                                             ▼
                              λ, kritischer Kreis, Drift = λ - T
                                             │
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                   Monte Carlo          What-if            CI-Gate
                   λ_p50, λ_p95         Ranking            Exit-Code
```

Die Trennung ist bewusst hart: **Der Mathe-Kern kennt weder Airflow noch dbt.** Er sieht nur Tasks mit Dauern, Intra-Kanten und Cross-Run-Kanten. Damit ist er rein testbar, ohne dass eine DAG-Datei existieren muss, und ein dritter Parser (Dagster, Prefect) ist später eine reine Zusatzdatei.

## Der zentrale Datentyp

Die gesamte Schnittstelle zwischen Parser-Schicht und Mathe-Schicht ist eine einzige Struktur:

```python
@dataclass(frozen=True)
class CrossEdge:
    src: str                                 # Task im Lauf k - periods
    dst: str                                 # Task im Lauf k
    periods: int = 1

@dataclass(frozen=True)
class Pipeline:
    durations: dict[str, float]              # Task-Name → Dauer
    intra: list[tuple[str, str]]             # (von, nach) im selben Lauf
    cross: list[CrossEdge]
```

`periods` ist der Versatz in Schedule-Perioden. Der Prototyp kennt ihn nicht, er nimmt implizit immer 1 an. Für `execution_delta = 2 * Periode` braucht es ihn, und er verändert die Mathematik: eine Kante mit Versatz n zählt im Zyklusmittel als n Kanten, halbiert bei n = 2 also den Beitrag des Kreises. Herleitung in [decisions.md](decisions.md), ADR-006.

Die Validierung sitzt im `__post_init__` von `Pipeline` und ist damit die Systemgrenze zwischen Parser-Schicht und Mathe-Schicht: unbekannter Task-Name in einer Kante, negative Dauer, `periods < 1` und ein zyklischer Intra-Run-Graph werfen sofort. Innerhalb des Kerns wird nicht mehr geprüft.

## Was der Scanner nicht ist

Der Scanner berechnet **kein λ**. Er weiß nichts über Dauern, weil öffentliche Repos keine Laufzeit-Historie mitliefern. Er beantwortet nur: existiert eine Cross-Run-Kante, und taktet der Schedule sub-täglich. Das ist die Risiko-Kandidaten-Quote, und mehr darf daraus nicht behauptet werden. Ein Satz wie "N Prozent der Pipelines sind instabil" wäre gelogen. Richtig ist: "N Prozent haben die Struktur, in der Instabilität überhaupt entstehen kann, und keines der Tools zeigt ihnen, ob sie es sind."
