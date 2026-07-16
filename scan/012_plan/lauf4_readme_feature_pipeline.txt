eigenlag analyze
================

DAG:        feature_pipeline (dags/feature_pipeline.py:6, schedule '@hourly')
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
  feature_pipeline.train_model -> feature_pipeline.train_model: weight 4000 s (66.67 min), 1 period back [wait_for_downstream, dags/feature_pipeline.py:4]
    as task path: feature_pipeline.build_features -> feature_pipeline.train_model
Resolved across all segments: feature_pipeline.build_features -> feature_pipeline.train_model
The path to a smaller λ runs through this cycle; a shortening anywhere else changes λ by exactly zero. Whether a single shortening carries through or a second cycle with the same cycle mean takes over is what the what-if ranking below computes.

Acceleration plan
-----------------
Base: λ = 4000 s (66.67 min). Each action is unclaimed reserve, sorted by the new λ.
  1. cross-run edge feature_pipeline.train_model -> feature_pipeline.build_features removed: λ 2000 s (33.33 min), -2000 s (-50 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: usually guards against overlapping writes; with partition isolation (each run writes its own partition) overlap is safe.
  2. task feature_pipeline.build_features halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan gives the arithmetic here, not detail about the foreign task.
  3. task feature_pipeline.train_model halved (to 1000 s): λ 3000 s (50 min), -1000 s (-25 %)
     makes your current schedule sustainable and removes the 400 s (6.67 min) of drift per run.
     commonly resolved by: split the task, shrink the increment, or warm-start; the plan gives the arithmetic here, not detail about the foreign task.
  2 more scenarios leave λ unchanged: 2 edges off the critical cycle.
A change that does not lie on the critical cycle changes λ by exactly zero. The plan therefore computes the cycle tasks and all cross-run edges; what does not change λ is irrelevant to the cycle limit, however useful it may be for the latency of a single run.

Monte Carlo
-----------
Not computed (disabled or no cycle). The λ above is a point value on the chosen statistic.

Warnings
--------
  - Duration assumed: feature_pipeline.build_features (no measurement, 2000.0 s)
  - Duration assumed: feature_pipeline.train_model (no measurement, 2000.0 s)

Model limits
------------
  - Unbounded parallelism assumed: λ is a lower bound of the real cycle time. The tool says 'no faster than λ', not 'λ is achievable'.
  - Retries, sensor poking and pool limits are not modeled. They can only raise the real cycle time, never lower it; the lower bound stays valid.
  - Latency figures are makespan: the duration of one run from its start to the end of its longest path, not the delay against the plan.

