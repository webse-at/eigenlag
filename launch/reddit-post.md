# DRAFT — David redigiert. Nicht posten.

Ziel: r/dataengineering. Selbst-Autorenschaft ist im letzten Absatz offengelegt;
vor dem Posten die aktuellen Subreddit-Regeln zu Self-Promotion prüfen. Alle
Zahlen stammen aus `wikimedia/case.md` (jede mit PromQL und Permalink belegt).
Die Kommentar-Strategie steht in `wiki/positioning.md` und gehört nicht in den Post.

---

## Title

I measured 30 production Airflow DAGs whose median runtime exceeds their schedule interval. 29 of them are fine. Here is what actually decides it.

## Body

Wikimedia runs Airflow in production, and both parts of it are public: the DAG code on GitLab and the measured run durations in an anonymously queryable Prometheus. I used that to check a scheduling question across a whole organization, on real data.

The sweep covers 453 DAG/instance rows; for 249 of them the planned interval is known. 30 run longer than their interval in the median. If "runtime over schedule" were a useful alarm, all 30 should be falling behind. 29 are not.

The reason is overlap. When runs are independent of each other, run k simply starts while run k−1 is still going. An hourly DAG whose runs take 90 minutes keeps two runs in flight and still delivers one result per hour, indefinitely. A monitoring rule that compares runtime against the schedule produces 29 false alarms for the one real finding.

What separates the one real case from the 29 is a dependency across runs: its next run waits on something from the previous one. In Airflow that edge comes from `depends_on_past`, `wait_for_downstream`, `max_active_runs=1`, or an `ExternalTaskSensor` with a time offset; in dbt it comes from an incremental model that reads its own target table. Once such an edge exists, the DAG contains a loop over the time axis, and a loop has a shortest period it can sustain.

The clearest picture of that loop is a sourdough starter. Part of yesterday's starter goes into today's dough, and the rest needs twelve hours before it is usable again. Ten ovens and twenty bakers change nothing about how often bread can be baked, because the starter waits on itself.

In the sweep itself the one real finding is an hourly DAG held back by an `ExternalTaskSensor` edge. The cleanest illustration of the steady state, though, is a pair of reconcile DAGs that the sweep table cannot even list, because their `dag_id` is generated dynamically. One of them shows what a pipeline on its cycle limit looks like. `wdqs_streaming_updater_reconcile_hourly` runs hourly with `depends_on_past=True` and `max_active_runs=1` ([code, pinned to the commit](https://gitlab.wikimedia.org/repos/data-engineering/airflow-dags/-/blob/6d0cceb85e4a21d593638f6b9e5694e5f4dbc013/search/dags/rdf_streaming_updater_reconcile.py#L112-121)). Over 398 runs its mean duration is 3598.4 s against a 3600 s interval, and its schedule delay holds at a median of 48 minutes. The delay does not grow, and it does not recover either. The pipeline sits exactly on its cycle limit and pays a 48-minute penalty for it, every hour. One honest caveat: its sensors wait for the current hour's data, so a run that starts late finds its data already there and finishes faster. That negative feedback is part of why this system settles instead of drifting, and it also means the measured durations are already the result of the steady state.

The number that decides all of this is computable up front. The loop's shortest sustainable period is the maximum cycle mean of the dependency graph, its max-plus eigenvalue; compiler people know the same idea as RecMII in modulo scheduling. Schedule faster than that and the delay grows by the difference on every run, no matter how many workers you add. Schedule slower and delays from a bad run fade out on their own.

Disclosure: I wrote the case study and a small open-source CLI that computes this bound from DAG files (AST-based, with the file and line for every edge it claims). If you want to check whether one of your pipelines has a sourdough in it: https://github.com/webse-at/eigenlag. Running `eigenlag demo` shows a full report without touching your DAGs. The Wikimedia case study with every query and permalink is in the repo. Corrections are very welcome, especially of the "this is wrong because" kind.
