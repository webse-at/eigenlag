# Review-Paket Launch-Texte — für externes Korrekturlesen (Gemini)

## Kontext für den Reviewer

**Produkt:** `eigenlag`, ein Open-Source-CLI (Python), das für Apache-Airflow-Pipelines die nachhaltige minimale Taktzeit berechnet (Max-Plus-Eigenwert λ): die harte Untergrenze, unter die ein Schedule nicht gedrückt werden kann, egal wie viele Worker laufen. Kern-Verkaufsargumente: (1) ein Vorher/Nachher-„Beschleunigungsplan" je Pipeline („diese kostenlose Architektur-Änderung rettet deinen Takt, das GPU-Upgrade nicht"), (2) eine Fallstudie auf öffentlichen Wikimedia-Produktionsdaten (30 DAGs laufen länger als ihr Takt, 29 davon sind trotzdem stabil — „Laufzeit über Takt" ist als Alarm wertlos), (3) ein CI-Gate.

**Zielgruppe:** Data Engineers (Airflow/dbt), skeptisch gegenüber Marketing, belesen in r/dataengineering. Vertrauen entsteht durch Belegbarkeit und ehrliche Grenzen, nicht durch Superlative.

**Was der Review leisten soll, je Text:**
1. Sprachliche Korrektur (Englisch: Grammatik, Idiomatik, Deutschismen — der Autor ist deutschsprachig).
2. Klarheit: Sätze, die man zweimal lesen muss, umbauen.
3. KI-Klang-Prüfung: Formulierungen markieren, die nach LLM klingen (aufgeblähte Übergänge, symmetrische Antithesen, „delve/leverage/robust"-Vokabular, Gedankenstrich-Ketten).
4. Kanal-Passung: Reddit-Post wie ein Praktiker-Beitrag, nicht wie ein Blogpost; Mail höflich-knapp; Release-Notes nüchtern.

**Was der Review NICHT tun soll:**
- Keine Marketing-Sprache hinzufügen (kein „game-changing", „blazingly fast", keine Emojis, keine Ausrufezeichen-Rhetorik).
- Zahlen, Fachbegriffe und Links nicht verändern (λ, cross-run, DAG, max-plus, RecMII, alle Messwerte bleiben exakt wie sie sind).
- Die ehrlichen Einschränkungen (Caveats, Limitations) nicht abschwächen oder streichen — sie sind Absicht.
- Die Offenlegung der Autorenschaft im Reddit-Post nicht entfernen.

**Gewünschtes Ausgabeformat:** je Text eine nummerierte Liste konkreter Änderungen: Zitat der Original-Stelle → Vorschlag → ein Halbsatz warum. Am Ende je Text ein Satz Gesamturteil. Keine komplette Neufassung, außer ein Absatz ist wirklich nicht zu retten.

Die deutschen Kopfzeilen (`# DRAFT …`) sind interne Notizen und nicht Teil des zu prüfenden Textes.

---

# Text 1: Reddit-Post (r/dataengineering)

# DRAFT — David redigiert. Nicht posten.

Ziel: r/dataengineering. Selbst-Autorenschaft ist im letzten Absatz offengelegt;
vor dem Posten die aktuellen Subreddit-Regeln zu Self-Promotion prüfen. Alle
Zahlen stammen aus `wikimedia/case.md` (jede mit PromQL und Permalink belegt).
Die Kommentar-Strategie steht in `wiki/positioning.md` und gehört nicht in den Post.

---

## Title

I measured 30 production Airflow DAGs whose median runtime exceeds their schedule interval. 29 of them are fine. Here is what actually decides it.

## Body

Wikimedia runs Airflow in production, and both halves of it are public: the DAG code on GitLab and the measured run durations in an anonymously queryable Prometheus. I used that to check a scheduling question across a whole organization, on real data.

The sweep covers 453 DAG/instance rows; for 249 of them the planned interval is known. 30 run longer than their interval in the median. If "runtime over schedule" were a useful alarm, all 30 should be falling behind. 29 are not.

The reason is overlap. When runs are independent of each other, run k simply starts while run k−1 is still going. An hourly DAG whose runs take 90 minutes keeps two runs in flight and still delivers one result per hour, indefinitely. A monitoring rule that holds runtime against the schedule produces 29 false alarms for the one real finding.

What separates the one real case from the 29 is a dependency across runs: its next run waits on something from the previous one. In Airflow that edge comes from `depends_on_past`, `wait_for_downstream`, `max_active_runs=1`, or an `ExternalTaskSensor` with a time offset; in dbt it comes from an incremental model that reads its own target table. Once such an edge exists, the DAG contains a loop over the time axis, and a loop has a shortest period it can sustain.

The clearest picture of that loop is a sourdough starter. Part of yesterday's starter goes into today's dough, and the rest needs twelve hours before it is usable again. Ten ovens and twenty bakers change nothing about how often bread can be baked, because the starter waits on itself.

In the sweep itself the one real finding is an hourly DAG held back by an `ExternalTaskSensor` edge. The cleanest illustration of the steady state, though, is a pair of reconcile DAGs that the sweep table cannot even list, because their `dag_id` is assembled at call time. One of them shows what a pipeline on its cycle limit looks like. `wdqs_streaming_updater_reconcile_hourly` runs hourly with `depends_on_past=True` and `max_active_runs=1` ([code, pinned to the commit](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L112-121)). Over 398 runs its mean duration is 3598.4 s against a 3600 s interval, and its schedule delay holds at a median of 48 minutes. The delay does not grow, and it does not recover either. The pipeline sits exactly on its cycle limit and pays 48 minutes every hour for it. One honest caveat: its sensors wait for the current hour's data, so a run that starts late finds its data already there and finishes faster. That negative feedback is part of why this system settles instead of drifting, and it also means the measured durations are already the result of the steady state.

The number that decides all of this is computable up front. The loop's shortest sustainable period is the maximum cycle mean of the dependency graph, its max-plus eigenvalue; compiler people know the same idea as RecMII in modulo scheduling. Schedule faster than that and the delay grows by the difference on every run, no matter how many workers you add. Schedule slower and delays from a bad run fade out on their own.

Disclosure: I wrote the case study and a small open-source CLI that computes this bound from DAG files (AST-based, with the file and line for every edge it claims). If you want to check whether one of your pipelines has a sourdough in it: https://github.com/webse-at/eigenlag. Running `eigenlag demo` shows a full report without touching your DAGs. The Wikimedia case study with every query and permalink is in the repo. Corrections are very welcome, especially of the "this is wrong because" kind.

---

# Text 2: Mail an Wikimedia (Data-Platform-Team)

# DRAFT — David redigiert. Nicht senden.

Empfänger-Kanal, zwei Wege (beide vor dem Senden verifizieren, die Liste zuerst):

1. **Öffentliche Mailing-Liste** des Analytics/Data-Engineering-Umfelds
   (historisch `analytics@lists.wikimedia.org`; aktuelle Liste unter
   lists.wikimedia.org prüfen).
2. **Phabricator-Task** unter dem Tag des Data-Platform-/Data-Engineering-Teams
   (phabricator.wikimedia.org), gleicher Text als Task-Beschreibung.

Unter 200 Wörter, kein Pitch. Der Repo-Link funktioniert erst, wenn das Repo
public ist (Checkliste beachten).

---

Subject: Analysis of your public Airflow metrics — heads-up and request for corrections

Hello,

I analyzed the scheduling behaviour of your production Airflow instances, using only public sources: the DAG code on your GitLab and the run-duration metrics in your anonymously queryable Prometheus. The full case study, with the PromQL and a commit-pinned permalink for every number, is here:

https://github.com/webse-at/eigenlag/blob/main/wikimedia/case.md

Two findings might interest you:

1. `wdqs_streaming_updater_reconcile_hourly` sits in a stable equilibrium at its cycle limit: over 398 runs, mean duration 3598.4 s against the 3600 s interval, with a constant median schedule delay of 48 minutes. Nothing looks broken; it is the steady state of a run-to-run feedback loop, and the 48 minutes are its price. Whether that price matters for reconciliation is your call.

2. Of 30 DAGs whose median runtime exceeds their schedule interval, 29 do not fall behind, because their runs may overlap. Runtime versus schedule alone would be a poor alert.

I plan to write publicly about this and wanted you to see it first. If any number or reading is wrong, I would like to correct it before that.

Thank you for keeping this data public. That is rare, and it made this analysis possible.

David Paci

---

# Text 3: Kurztext Apache-Airflow-Community-Slack

# DRAFT — David redigiert. Nicht senden.

Ziel: Apache-Airflow-Community-Slack (apache-airflow.slack.com). Kanal vor dem
Posten prüfen: ein Kanal für Community-Inhalte/Show-and-Tell, nicht der
Support-Kanal. Selbst-Autorenschaft ist im Text offen.

---

I measured 30 of Wikimedia's production DAGs whose median runtime exceeds their schedule interval, from their public code and metrics. 29 of them are fine because their runs overlap; what sets the real case apart is an edge across runs. The clearest example is an hourly reconcile DAG that sits exactly on its cycle limit and pays a constant 48 minutes of delay as the price of its steady state. I wrote the analysis up with a permalink for every number, and built a small open-source CLI that computes the sustainable cycle-time bound from DAG files: https://github.com/webse-at/eigenlag. Corrections welcome.

---

# Text 4: GitHub-Release-Notes v0.1.0

# DRAFT — David redigiert. Text für das GitHub-Release v0.1.0 (Tag setzt David).

---

## eigenlag 0.1.0

First release. eigenlag computes the sustainable minimum cycle time of an Airflow pipeline: the max-plus eigenvalue λ of its cross-run dependency graph, which is the hard lower bound no amount of workers can beat.

**What it does:**

- `eigenlag analyze PATH` reads DAG files (Python `ast`, never regex), joins task durations from the Airflow metadata DB, the REST API or a flat assumption, and reports: the verdict (stable, unstable, at the limit), the critical cycle down to file and line, an acceleration plan that prices every possible change, and a Monte Carlo range for λ. English by default, German with `--lang de`, machine-readable with `--json`.
- `eigenlag check PATH --against REF` is a CI gate: it compares λ and the cross-run edge set of a pull request against a git reference and fails before a change pushes the pipeline over its cycle limit.
- `eigenlag demo` renders the full report of a built-in example pipeline, in under a second, without reading any files. Start there.

**What it deliberately does not do:**

- λ assumes unbounded parallelism and is a lower bound; retries, sensor poking and pool limits can only raise the real cycle time.
- Sensors that wait on the current period couple a pipeline to the wall clock; the tool marks them on the critical cycle instead of quietly trusting their measured durations.
- What the parser cannot resolve statically becomes a warning, never a guess.

The claim behind the tool is tested against production data: a case study of Wikimedia's public Airflow code and metrics, with the PromQL and a commit-pinned permalink for every number, is in [wikimedia/case.md](https://github.com/webse-at/eigenlag/blob/main/wikimedia/case.md).

Zero runtime dependencies. `pipx install eigenlag`, Python 3.12+.

---

# Text 5: README.md (die Repo-Startseite — wichtigster Marketing-Text)

# eigenlag

[![CI](https://github.com/webse-at/eigenlag/actions/workflows/ci.yml/badge.svg)](https://github.com/webse-at/eigenlag/actions/workflows/ci.yml)

Computes the sustainable minimum cycle time of an Airflow pipeline: the hard lower
bound no amount of workers can beat. When a run depends on an earlier run, the
pipeline has a cycle across the time axis, and that cycle has a shortest period it
can hold. That period is the max-plus eigenvalue λ. If you schedule faster than λ,
the delay grows every run, forever, and no dashboard tells you why.

![eigenlag demo: the built-in example report, ending on the acceleration plan](assets/demo.gif)

Development docs are in German (`wiki/`), the tool speaks English by default and
German with `--lang de`.

## The sourdough

A bakery bakes sourdough bread. Every morning part of yesterday's starter goes into
today's dough, the rest is fed and needs twelve hours before it can rise again.

The baker can buy ten ovens and hire twenty people. They still cannot bake more
often than every twelve hours, because the starter waits on itself. Capacity is not
the bottleneck, the loop is. Those twelve hours are λ.

Every pipeline with `depends_on_past`, `wait_for_downstream`, an incremental dbt
model or a cross-DAG sensor with a time offset has a starter. Nobody knows how long
it has to rise.

## What the tool computes

When run k waits on run k-1, a cycle forms across the time axis. That cycle has a
period λ, the max-plus eigenvalue of the dependency graph. λ is the shortest cycle
time the pipeline can sustain.

Given a schedule with period T:

- **T ≥ λ**: stable, delays fade out.
- **T < λ**: every run starts `λ - T` later than the previous one. The delay grows
  linearly and without bound. More workers change nothing.

Today's tools show the critical path of a single run. That is a different number: the
latency of one pass through the DAG, not the rate at which passes can follow each
other without piling up.

## Quickstart

Requires Python 3.12+.

```
pipx install git+https://github.com/webse-at/eigenlag
```

Point it at a DAG file or a directory. Durations come from the Airflow metadata DB
(`--db`), the REST API (`--rest`), or a flat assumption (`--assume-duration`); without
a duration source the tool refuses to guess.

```
eigenlag analyze dags/feature_pipeline.py --assume-duration 2000
```

For a pipeline whose tasks carry `wait_for_downstream` on an hourly schedule, the
report leads with the verdict and backs every claim with its source:

```
eigenlag analyze
================

DAG:        feature_pipeline (dags/feature_pipeline.py:6, schedule '@hourly')
Period T:   3600 s (60 min), source: schedule '@hourly'
Durations:  assumed: 2000 s per task without a measurement

Verdict
-------
Unstable: λ = 4000 s (66.67 min) lies above the period T = 3600 s (60 min). The
delay grows by 400 s (6.67 min) per run, without bound and regardless of the number
of workers. One hour of backlog is reached after 9 runs. More compute changes
nothing, because the bottleneck is the dependency structure, not the capacity.

Critical cycle
--------------
Condensed (the cycle in the cross-run matrix; its cycle mean is λ):
  feature_pipeline.train_model -> feature_pipeline.train_model: weight 4000 s, 1 period back [wait_for_downstream, dags/feature_pipeline.py:4]
    as task path: feature_pipeline.build_features -> feature_pipeline.train_model
Resolved across all segments: feature_pipeline.build_features -> feature_pipeline.train_model

Acceleration plan
-----------------
Base: λ = 4000 s (66.67 min). Each action is unclaimed reserve, sorted by the new λ.
  1. cross-run edge feature_pipeline.train_model -> feature_pipeline.build_features removed: λ 2000 s (33.33 min), -2000 s (-50 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: usually guards against overlapping writes; with partition isolation (each run writes its own partition) overlap is safe.
  2. task feature_pipeline.build_features halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan shows the arithmetic, whether and how to split is yours to judge.
  3. task feature_pipeline.train_model halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan shows the arithmetic, whether and how to split is yours to judge.
  2 more scenarios leave λ unchanged: 2 edges off the critical cycle.
```

The acceleration plan turns the diagnosis into action: it states every finding as
unclaimed reserve. Removing the `wait_for_downstream` edge, which costs nothing, makes
the current schedule sustainable and clears the 400 s of drift per run; each entry
names the usual way that kind of edge is resolved, without claiming to know the task.
When the pipeline is already stable, the same section reads the other way and puts a
number on the headroom: how many more times a day it could run and how much fresher
the data would stay. A change off the critical cycle moves λ by exactly zero, however
much it helps a single run's latency, and every number carries its own source, down to
the file and line that produced the edge.

## CI gate

`eigenlag check` compares λ and the cross-run edge set of your working tree against a
git reference and fails a pull request before a change that pushes the pipeline over
its cycle limit is merged. It reads no network and never posts anything itself; the
comment goes to stdout or to `--comment-file`, and the CI job posts it.

```yaml
name: eigenlag-gate
on:
  pull_request:
    paths: ["dags/**"]
jobs:
  check:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0            # the gate needs the base reference in the clone
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install eigenlag
      - id: gate
        run: eigenlag check dags --against "origin/${{ github.base_ref }}" --comment-file comment.md
      - if: always() && steps.gate.outcome != 'skipped'
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          path: comment.md
```

Without a duration source the gate runs in structural mode: a new cross-run edge that
closes a cycle at a sub-daily schedule fails the build. Add `--db "$AIRFLOW_DB_URL"`
to turn the structural comparison into a comparison in seconds against the period.
The full reference is in [docs/ci-gate.md](docs/ci-gate.md).

## What it will tell you

The point is less the single λ value than the distinction it draws. A tool that only
holds runtime against the schedule cannot tell a structural problem from a capacity
one, and it produces false alarms.

We measured this on Wikimedia's production Airflow, which is public. Of the DAGs that
run longer than their schedule in the median, 30 are sub-daily. 29 of them do not
drift, because their runs are allowed to overlap. Only one sits on its cycle limit
and pays a constant delay every run. "Runtime over schedule" as a diagnosis is 29
false alarms to one real finding; λ is the number that separates them. The full
derivation, with PromQL and a permalink to every figure, is in
[wikimedia/case.md](wikimedia/case.md).

## Limitations

This section is not hidden under a FAQ, because for this audience it is the strongest
signal of what the number is worth.

- **Unbounded parallelism.** λ is a lower bound. The tool says "no faster than λ",
  not "λ is achievable". Retries, sensor poking and pool limits are not modeled; they
  can only raise the real cycle time, never lower it.
- **Clock-synchronised feedback.** A pipeline whose sensors wait on data from the
  current period couples to the wall clock and settles exactly at its cycle limit.
  Its measured durations are then already the result of that steady state, so λ can
  be overestimated. The tool marks sensors on the critical cycle instead of quietly
  trusting them (`wiki/math.md`, section 9).
- **Static analysis.** The parser reads Airflow DAG files with Python's `ast`, never
  regex. What it cannot resolve statically (a dynamic task id, a schedule computed at
  runtime, an edge into a dynamically mapped task) appears as a warning, never as a
  guess.

## How it works

Cross-run dependencies are encoded as a max-plus matrix; its eigenvalue is the
maximum cycle mean of the dependency graph, which is λ. Two independent algorithms
compute it and cross-check each other: Karp's minimum-cycle-mean and Howard's policy
iteration, the latter returning the critical cycle directly. The derivation is in
[wiki/math.md](wiki/math.md).

The tool has zero runtime dependencies; the math core is pure Python. Development uses
`pytest`, `ruff` and `mypy`. `sqlalchemy` is an optional extra (`eigenlag[db]`) for
reading the Airflow metadata DB.

## License

MIT. See [LICENSE](LICENSE).
