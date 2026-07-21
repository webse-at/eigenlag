# Cross-run signals A to G

This page defines exactly what counts as a cross-run edge. It is the shared basis for the scanner (phase 1) and the parser (phase 2). If scanner and parser use different definitions, the market figures and the product figures are not comparable, and the tool refutes its own launch content.

**Note on provenance:** The original brief refers to "signals A to F as in the prototype". The prototype `crossrun_scan.py` does not exist (see ADR-001). The following list is therefore our own definition, derived from Airflow and dbt semantics. It thus requires justification, it is not inherited.

## The signals

### A — `depends_on_past=True`

A task in run k starts only once the same task in run k-1 succeeded.

**Graph effect:** self-edge `task(k-1) → task(k)` with weight equal to the duration of the task.

**Where found:** directly on the operator, or in `default_args`, in which case it applies to all tasks of the DAG. Both levels have to be detected, and the operator level overrides `default_args`.

**Pitfall:** an explicit `depends_on_past=False` is a hit in the regex sense and **not** a signal. Only the AST distinguishes that reliably.

### B — `wait_for_downstream=True`

A task in run k starts only once the task **and all of its direct downstream tasks** in run k-1 succeeded. Implies `depends_on_past`.

**Graph effect:** edge from every direct downstream successor in run k-1 onto the task in run k. This is strictly stronger than A and usually produces a markedly longer cycle, because the path through the downstream tasks counts too.

### C — `ExternalTaskSensor` with a time offset

A sensor waits on a task in a **different** DAG.

**Cross-run only if** `execution_delta` or `execution_date_fn` is set. Without either, the sensor points at the same logical date, which is an intra-run edge between two DAGs and **not** a signal.

**Also not a signal: an offset of zero.** `execution_delta=timedelta(hours=0)` is set, but points at the same logical date and is thus the same intra-run edge as a missing offset. A `timedelta` literal is therefore evaluated, instead of merely being checked for presence (ADR-014, found in the sample for session 003). An offset that cannot be resolved statically still counts.

**Graph effect:** edge from the target task of the foreign DAG at logical date `t - execution_delta` onto the sensor at `t`. If `execution_delta` is a multiple of the schedule period, this spans a cycle across several periods. An `execution_delta` of two periods produces a cycle with two edges, which halves the cycle mean. Exactly this case belongs as a test fixture in phase 2.

**Limit:** `execution_date_fn` is an arbitrary Python function. Statically its return value is in general not determinable. We count it as a cross-run signal (the time offset is its only purpose), but cannot derive the weight. The parser has to report that as "cross-run detected, offset unknown" and must not guess.

### D — `include_prior_dates=True`

Parameter on the `ExternalTaskSensor`. The sensor accepts runs with an earlier logical date too.

**Graph effect:** cross-run, because the dependency explicitly reaches into the past. Counted independently of C, because a sensor can have `include_prior_dates=True` without `execution_delta`.

### E — dbt `is_incremental()`

A dbt model with `materialized='incremental'` whose SQL calls `is_incremental()` reads, in the incremental run, from its own target table, typically via `select max(ts) from {{ this }}`.

**Graph effect:** self-edge `model(k-1) → model(k)`. The model cannot start before its own previous run has written.

**Pitfall:** `materialized='incremental'` without `is_incremental()` in the body is a full-refresh model in incremental disguise and not a real recurrence. Conversely, `is_incremental()` in a non-incremental model is dead code. Neither counts. Only the combination counts.

### F — prior-run templates

Jinja references to the previous run in templates, operator arguments or SQL: `prev_ds`, `prev_execution_date`, `prev_start_date_success`, `prev_data_interval_start_success`, `prev_data_interval_end_success`.

**Three locations, the same semantics** (ADR-013, from the negative search in session 003):

| Location | Example | Origin |
|---|---|---|
| String literal in the operator argument | `bash_command="load --since {{ prev_start_date_success }}"` | the regular case |
| Parameter name of the callable | `def load(prev_start_date_success, **kwargs)`, `lambda prev_start_date_success: ...` | Airflow injects the context via the parameter name (`oxylabs/building-scraping-pipeline-apache-airflow`, `DAG/scrape.py:26`) |
| Template in a module variable | `date_last_success = '{{ prev_start_date_success }}'` | interpolated into the operator argument later (`abdurahim-dag/portfolio`, `.../dags/init.py:42`) |

Whoever detects only the first location measures the spread of a spelling, not that of the signal.

**Graph effect:** the task reads data defined by the previous run. That is a genuine data dependency across the run boundary.

**Gradation:** the `*_success` variants are hard edges, because they refer to the **successful** previous run and thereby wait on its completion. `prev_ds` and `prev_execution_date` are pure date arithmetic without wait semantics: they indicate a data dependency but do not enforce an order. They are therefore treated as a **weak signal**, counted separately and **not** included in the risk-candidate rate. Whoever counts them inflates the market figure and hands the first critical reader the ammunition to knock it over.

### G — `max_active_runs=1`

```python
with DAG(dag_id="reconcile", schedule="@hourly", max_active_runs=1) as dag:
```

**Graph effect:** run k cannot begin before run k−1 is finished. `end(k−1) ≤ start(k)` is an edge across the time axis that spans the **entire** run and is therefore often the binding one.

This signal was in the table below until session 005, i.e. under "not a cross-run signal", on the grounds that it limits concurrency and not recurrence. The Wikimedia case refuted that: `wdqs_streaming_updater_reconcile_hourly` delivers its runs 3599.5 seconds apart at a mean run duration of 3598.4 seconds, so the runs lie back to back. The distinction between data and resource dependency is meaningless for the eigenvalue, it sees edges. See ADR-016.

**Only the explicit `1` counts.** Airflow's default is larger and lets runs run side by side. An expression that cannot be resolved statically does not count.

## Translation into λ edges (parser, phase 2)

Since session 007, `eigenlag/parse_airflow.py` translates the signals into edges of the max-plus graph. The guiding rule: the parser may know less than what is in the file, but never more. What cannot be resolved statically becomes a warning with file and line, not an edge — omission is the safe direction, λ remains a valid lower bound (math.md, section 8).

| Signal | λ edge | Justification |
|---|---|---|
| A on task t | `CrossEdge(t, t, 1)` | task waits on its own previous instance |
| A in `default_args` | self-edge for every task; operator level overrides | Airflow inheritance semantics |
| B on task t | in addition to A: `CrossEdge(d, t, 1)` for every **direct** downstream d | t(k) waits on t(k−1) and its direct successors; only direct ones, that is how Airflow is defined |
| C with `execution_delta`, target in the parse set, same T, `delta/T` integer ≥ 1 | `CrossEdge(target, sensor, delta/T)`, target namespaced `dag_id.task_id` | the only edge with `periods > 1` (ADR-006) |
| C otherwise (target missing, T differs, ratio not integer, offset not resolvable) | no edge, warning `sensor_not_modeled` with the concrete reason | different periods are not representable in the one-period model; a lower bound is better than an invented edge |
| C with `execution_date_fn` | no edge, warning `sensor_dynamic_offset` | return value not determinable statically |
| D | no edge, warning `include_prior_dates` | "some earlier run is enough" is weaker than "the previous one has to be finished"; an edge would falsely raise λ |
| E | self-edge `model(k-1) → model(k)` | dbt parser, session 008 |
| F | **no edge**, finding `prev_run_success` / `prev_run_date` | the template renders and does not wait; market figure and λ model measure two different things (ADR-020) |
| G | `CrossEdge(s, q, 1)` for every sink s and every source q | run k starts only once run k−1 is completely finished; λ = makespan, consistent with ADR-019 |

## What is explicitly not a cross-run signal

| Construct | Why not |
|---|---|
| `ExternalTaskSensor` without a time offset | Points at the same logical date. Intra-run. |
| `depends_on_past=False` | Explicit negation. |
| `TriggerDagRunOperator` | Triggers a new run, but does not wait on the previous one. A chain, not a cycle. |
| `catchup=True` | Backfill behavior, not a dependency. |
| Sensor on an external data source | Waits on the world, not on its own previous run. |

## Schedule classification

A signal alone is harmless. It becomes dangerous only when the schedule ticks faster than the cycle allows. Since ADR-018 there are two separately reported risk classes:

- **Risk candidate (core):** at least one strong signal from A, B, C, D or F (`*_success` variants) **and** a sub-daily schedule **in the same DAG**. Here the cycle is a partial path, λ < makespan is possible, and no tool today answers that. This is the launch figure, defined identically to session 003.
- **Risk candidate (G only):** G as the only strong signal **and** sub-daily. The edge is real (ADR-016), but λ = makespan; runtime monitoring gives the same answer there (ADR-017). Its own row in the report, never mixed into the core rate.

A DAG with an A–F signal and G counts into the core class; G is reported as a column nonetheless. E (dbt) stays outside both classes, because a dbt model has no schedule (ADR-012). Weak are `prev_ds`, `prev_ds_nodash` and `prev_execution_date`; they are counted separately and on their own do not establish a risk candidate (ADR-005, ADR-011).

The corpus scan from session 003 knew neither signal G nor the constructors from ADR-015. Session 006 rescanned the corpus under the new definition (`scan/v2/report.md`): the core rate stayed identically defined, G stands beside it as its own class.

Sub-daily means: period shorter than 24 hours.

| Schedule form | Example | Sub-daily |
|---|---|---|
| Preset | `@hourly` | yes |
| Preset | `@daily`, `@weekly`, `@monthly` | no |
| Preset | `@once`, `None` | no, no schedule |
| Preset | `@continuous` | yes, the period is by definition below a day |
| Cron, minute field with step | `*/15 * * * *` | yes |
| Cron, hour field with step | `0 */6 * * *` | yes |
| Cron, hour field as a list | `0 6,18 * * *` | yes |
| Cron, fixed hour | `0 3 * * *` | no |
| `timedelta` | `timedelta(hours=4)` | yes |
| `timedelta` | `timedelta(days=1)` | no |
| Dataset- or asset-triggered | `schedule=[Dataset(...)]` | unknown, its own category |

Cron expressions are not classified heuristically from the string, but via the computed smallest distance between two consecutive firing times. That is the only method that stays reliable with lists, steps and combinations, and it is testable with a table of examples. Implemented in `scanner/schedule.py`, computed over a window of five years, without a cron library (ADR-010). An expression that never fires in this window is `unknown` and is not guessed.
