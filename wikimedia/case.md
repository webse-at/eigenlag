# The first real case: Wikimedia's pipelines, measured against their schedule

Wikimedia runs Airflow in production, and both parts of it are public: the DAG code on
GitLab, the measured run durations in an anonymously queryable Prometheus. That makes it
possible to test the project's thesis against real data for the first time.

**The load-bearing finding is the sweep across the organization: 30 DAGs run longer than
their schedule interval in the median, and 29 of them do not drift**, because their runs are
allowed to overlap. Those 29 would be the false alarms of any tool that merely holds runtime
against the schedule. "Runtime over schedule" is worthless as a diagnosis; what matters is
whether an edge across the time axis serializes the runs. The table for this is in section 6.

The single case behind it: `wdqs_streaming_updater_reconcile_hourly` runs on an hourly
schedule (T = 3600 s) with `depends_on_past=True` and `max_active_runs=1`, and its mean run
duration is 3598.4 seconds. That the mean duration lands almost exactly on the schedule
interval is not a balancing act, it is the steady state of a feedback-coupled system
(section 4). The price of that state is a delay of 48 minutes that no longer grows and no
longer goes away.

**What this case proves, and what it does not:** It proves the thesis. A pipeline with a
cycle across the time axis cannot run its schedule faster than that cycle, and it pays for it
with a constant delay. It does not validate the eigenvalue machinery: at the DAG level,
without task durations, the graph is a single node with a self-edge, and λ is the run
duration itself (ADR-019, section 3).

All figures below come from `data/wikimedia/case_numbers.json`, produced by
`python -m wikimedia.case`. Every raw response is in `data/wikimedia/cache/`.

---

## 1. The code

Repo: `https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags.git`, at
`6d0cceb85e4a21d593638f6b9e5694e5f4dbc013` (July 14, 2026). The permalinks below point at
exactly this commit.

The DAG is built in [`search/dags/rdf_streaming_updater_reconcile.py`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L112-121):

```python
112    with create_easy_dag(
113            dag_id=dag_id,
114            start_date=datetime(2024, 2, 20, 7, 00, 00),
115            default_args={
116                'depends_on_past': True,
117            },
118            schedule='@hourly',
119            # We want hourly runs to be scheduled one ofter the other
120            max_active_runs=1,
121            catchup=True,
```

The function `build_dag` ([line 106](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L106))
is called twice and creates two DAGs:
[`wdqs_…`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L140)
and [`wcqs_…`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L147).
The task graph of a single run ([line 135](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L135)):

```python
wait_for_data >> job >> complete
```

Three Hive partition sensors wait on the current hour's data, a Spark job writes the
reconciliation events, an empty task closes the run.

**Two edges carry this DAG in a cycle across the time axis:**

- `depends_on_past: True` (line 116) ties each task to the same task of the previous run.
- `max_active_runs=1` (line 120) ties the whole run to the previous one: run k cannot begin
  before run k−1 is finished. The comment above it says so itself: *"We want hourly runs to
  be scheduled one ofter the other."*

The schedule is `@hourly`. `eigenlag` derives **T = 3600 s** from it (the smallest distance
between two firing times of `0 * * * *`, see `scanner/schedule.py:period_seconds`), instead
of entering the number by hand.

## 2. The measurement

Data source: `https://grafana.wikimedia.org/api/datasources/proxy/uid/000000026/api/v1`,
anonymously queryable. Window: 30 days, ending 2026-07-15 00:00 UTC.

### The gauge problem, and why `avg_over_time` is no good here

`airflow_dagrun_duration` is a gauge: at the end of a run Airflow reports its duration to
StatsD, the exporter holds the value, Prometheus scrapes it. An `avg_over_time` over it
averages **scrapes, not runs**. Whether that is the same thing depends on how long each value
stays put, and that is an assumption, not a guarantee.

That is why `wikimedia/runs.py` reconstructs the runs itself: **a value change of the gauge is
a run.** The durations are floating-point milliseconds with decimal places, two runs with
exactly the same value are practically ruled out. The change is looked for per series, not on
the merged timeline, because Wikimedia runs several StatsD pods and each holds its own last
value.

**Two independent checks that the reading holds:**

1. Computed server-side, `sum by (dag_id) (changes(airflow_dagrun_duration{state="success"}[30d]))`
   gives **397** for wdqs, our reconstruction from the raw samples gives **398**. Two methods,
   the same number.
2. The median run duration (3733.8 s) and the median gap between two run endings (3720 s)
   are 13.8 seconds apart, so below the scrape resolution of one minute. They have to be, if
   `max_active_runs=1` really lays the runs end to end, and the gap comes from the timestamps,
   not from the durations: one quantity checking the other.

The unit is thereby settled too: the values are milliseconds, not seconds, otherwise duration
and gap could not coincide.

### `wdqs_streaming_updater_reconcile_hourly`

PromQL: `airflow_dagrun_duration{dag_id="wdqs_streaming_updater_reconcile_hourly",state="success"}[30d]`

Gap-free window: 2026-06-15 00:50 to 2026-07-01 13:47 UTC, 16.5 days, **398 runs**.

| Quantity | Value |
|---|---|
| Schedule interval T (from `@hourly`) | 3600.0 s |
| Run duration, median | 3733.8 s (62.2 min) |
| Run duration, mean | **3598.4 s (60.0 min)** |
| Run duration, p95 | 3778.8 s (63.0 min) |
| Run duration, min / max | 104.6 s / 7432.5 s |
| Observed interval (gap between run endings) | 3599.5 s |

### `wcqs_streaming_updater_reconcile_hourly`

712 runs, spread over three windows (two metric outages in between), last run 2026-07-14
14:53 UTC. Median 3722.5 s, mean 3182.9 s, p95 3777.8 s. The mean is lower because this DAG
caught up after the outages; its longest run took 400,132 s, i.e. 4.6 days.

That single run shows how outlier-sensitive the mean is: it alone shifts the mean of the 712
runs by about 560 seconds. For the asymptotic drift the mean is nonetheless the correct
statistic, because every second of runtime feeds into the delay, including that of a stall.
It just must not be read as a typical run duration: a single stalling run poisons it. Whoever
computes λ on the mean has to see and name such runs, rather than smoothing them away.

**wdqs has reported no successful run since July 1, 2026.** The last success is on July 1 at
13:47 UTC, shortly after which there is a failure lasting 108.9 minutes. Since July 6 its
`airflow_dagrun_schedule_delay` gauge has held a single frozen value. Whether the DAG is
stalled, was paused, or is merely missing its metric cannot be decided from the outside, and
we do not claim it. The 398 runs before that are untouched by it.

## 3. λ

Model (`wikimedia/case.py:lambda_of`): at the DAG level both cycle edges collapse into one.
Run k cannot begin before run k−1 is finished, and the weight of the cycle is the run
duration. The graph has one node and one edge onto itself, and it is computed with `eigenlag`
(Howard, `eigenlag/maxplus.py`), not by hand.

**Named honestly: what λ is here.** On this graph, a single node with a self-edge, the
max-plus eigenvalue is by definition the edge weight. Condensation, Karp and Howard are an
identity function here: λ = 3598.4 s means the mean run duration is 3598.4 s. The case
therefore does not validate the eigenvalue machinery, it proves the thesis (ADR-019). That
does not make it weaker, it places it: that the cycle limit coincides with the run duration is
a property of this class of case, whose only binding edge spans the entire run (signal G). The
analyzer earns its keep only where the cycle is a partial path and λ can lie below the
makespan.

Why only the DAG level: the metric does not yield the task durations. For the Spark task and
the closing task **no** duration metric exists, `airflow_task_duration` carries neither
`dag_id` nor `task_id`, and the three sensors report durations near zero (median 0.0 min,
maximum 0.1 min), because they run in reschedule mode: their waiting is not part of their task
duration. Splitting the 62 minutes across the tasks would be a guess, and we do not guess.

| λ from | λ | Drift per run (λ − T) |
|---|---|---|
| median run duration | 3733.8 s | **+133.8 s** |
| mean run duration | 3598.4 s | **−1.6 s** |
| p95 run duration | 3778.8 s | +178.8 s |

For the question of whether the delay grows without bound, the **mean** is what counts: with
random run durations the mean cycle weight is the rate at which the delay increases per run
(`wiki/math.md`, section 7). That it lands almost exactly on the schedule interval is not a
narrow margin and not a coincidence, but the fixed point of a feedback-coupled system: the
later a run starts, the shorter it runs (correlation −0.504, section 4), so the system settles
exactly where the mean duration ≈ T. The measured durations are already the result of this
steady state.

The observed interval confirms it independently: the runs end on average **3599.5 s** apart.
The DAG delivers exactly one run per hour, more is impossible, and more is not needed either.
It sits on its cycle limit.

**The price is in the same metric.** `airflow_dagrun_schedule_delay` measures how long after
its logical time a run actually starts. Median: **2880 s, i.e. 48 minutes.** This delay no
longer grows, but it also does not go away. It is exactly what a pipeline at its cycle limit
shows: it holds the schedule, but permanently three quarters of an hour late.

## 4. Why the pipeline does not drift away despite a median above the schedule

The median run duration is 134 seconds above the schedule interval. Were the run durations
independent of the start time, the delay would have to grow. It does not, and the metric shows
why.

**Correlation between start delay and run duration: −0.504** (397 pairs, wdqs). The later a
run begins, the shorter it runs. The reason is in the code: the sensors wait on the Hive
partitions **of the current hour**. If a run starts on time, it waits for data that does not
exist yet. If it starts 50 minutes late, the data has long been there, and it is through in
minutes (shortest run: 104.6 s).

This sensor is therefore **not processing time, but a synchronization with the clock.** It
acts as negative feedback and breaks the cycle exactly when the delay has grown large enough.
This is why the pure max-plus assumption (durations independent of the start time) reaches its
limit here, and it is a limit one has to know before certifying drift for a pipeline. We wrote
it down in `wiki/math.md`, section 9.

For wcqs the same correlation is only −0.103, because two metric outages and a 4.6-day run
dominate the series there.

## 5. What the scanner did not see before

Before this session, `eigenlag` found **71 of 325 production DAGs and zero cross-run signals**
in Wikimedia's repo, even though `depends_on_past=True` is there in plain text several times.
Reason: Wikimedia does not create DAGs via `DAG(...)`, but via `create_easy_dag(...)`, a
method that internally returns a `DAG(...)`
([`wmf_airflow_common/easy_dag.py:79`](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/wmf_airflow_common/easy_dag.py#L79)).

| | before | after ADR-015 | after ADR-016 |
|---|---|---|---|
| DAGs found | 71 | 345 | 345 |
| of those with `dag_id` | 58 | 255 | 255 |
| with cross-run signal | 0 | 13 | 68 |
| risk candidates | 0 | 3 | 8 |

- **ADR-015**: A function that returns a `DAG(...)` is a DAG constructor of the repo. Found
  constructors: `create_easy_dag`, `create_easy_cassandra_loading_dag`.
- **ADR-016**: `max_active_runs=1` is itself a cross-run edge. Before, the scanner did not
  see the serialization on which this whole case rests at all.

A gap remains: 90 of the 345 DAGs have no `dag_id`, because only the calling function sets it
(`build_dag(dag_id=...)`). Our case DAG is one of them. We do not guess them.

## 6. The whole organization

`wikimedia/wikimedia_dags.csv`: 453 rows, one per (`dag_id`, Airflow instance). 406 run
measurably, 280 are in the code, 233 in both. For 249 we know the planned schedule interval.

**30 DAGs have a median run duration above their planned schedule interval.** For 29 of them
that is not drift: without `max_active_runs=1` and without a cross-run signal, two runs simply
run side by side. Precisely these 29 would be the false alarms of a tool that merely holds
runtime against the schedule. What remains is one with an edge across the time axis:

| dag_id | Interval | Median | Signal | Source |
|---|---|---|---|---|
| `mediarequest_hourly` | 3600 s | 6371 s | `external_task_sensor` | `main/dags/mediarequest/mediarequest_hourly_dag.py:46` |

Plus the two reconcile DAGs, which are missing from this table because their `dag_id` is not
in the code (see section 5).

## 7. What this case does not show

- **One organization is not a sample.** Wikimedia proves nothing about the market. The case
  shows that the computation works on real data and what it finds, not how common that is.
- **The gauge has limits.** Two runs with identical duration to the millisecond would count as
  one. For ten DAGs the gauge reports more value changes than its schedule allows
  (`refine_api_requests_hourly`: 3360 in 30 days on an hourly schedule). We do not know why,
  and for these DAGs we compute no λ.
- **Whether the backlog hurts Wikimedia, we do not know.** 48 minutes of delay on hourly
  reconciliation may be perfectly fine. We do not say something is broken here. We say that
  nobody computed this number before.
- **We did not contact Wikimedia.** Everything here comes from public sources, read-only,
  each query once and thereafter from the cache.
