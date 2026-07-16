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

DAG:        feature_pipeline (feature_pipeline.py:6, schedule '@hourly')
Period T:   3600 s (60 min), source: schedule '@hourly'
Durations:  assumed: 2000 s per task without a measurement
Statistic:  mean. For the asymptotic drift the mean is the theoretically correct quantity; it is sensitive to outliers, and a single hanging run can shift it noticeably.
Sample:     runs per task, minimum 0, median 0.

Verdict
-------
Unstable: λ = 4000 s (66.67 min) lies above the period T = 3600 s (60 min). The delay grows by 400 s (6.67 min) per run, without bound and regardless of the number of workers. One hour of backlog is reached after 9 runs (about 36000 s (10 h) of wall-clock time). More compute changes nothing, because the bottleneck is the dependency structure, not the capacity.

Critical cycle
--------------
Condensed (the cycle in the cross-run matrix; its cycle mean is λ):
  feature_pipeline.train_model -> feature_pipeline.train_model: weight 4000 s (66.67 min), 1 period back [wait_for_downstream, feature_pipeline.py:4]
    as task path: feature_pipeline.build_features -> feature_pipeline.train_model
Resolved across all segments: feature_pipeline.build_features -> feature_pipeline.train_model
The path to a smaller λ runs through this cycle; a shortening anywhere else changes λ by exactly zero. Whether a single shortening carries through or a second cycle with the same cycle mean takes over is what the acceleration plan below computes.

Acceleration plan
-----------------
Base: λ = 4000 s (66.67 min). Each action is untapped headroom, sorted by the new λ.
  1. cross-run edge feature_pipeline.train_model -> feature_pipeline.build_features removed: λ 2000 s (33.33 min), -2000 s (-50 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: usually guards against overlapping writes; with partition isolation (each run writes its own partition) overlap is safe.
  2. task feature_pipeline.build_features halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan shows the arithmetic; whether and how to split it is up to you.
  3. task feature_pipeline.train_model halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan shows the arithmetic; whether and how to split it is up to you.
  2 more scenarios leave λ unchanged: 2 edges off the critical cycle.
```

The acceleration plan turns the diagnosis into action: it frames every finding as
available headroom. Removing the `wait_for_downstream` edge, which costs nothing, makes
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

The single λ value matters less than the distinction it draws. A tool that only
compares runtime against the schedule cannot distinguish a structural problem from a capacity
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
