# Math: the max-plus eigenvalue as a cycle limit

This page is the reference for the math core. It deliberately stands before the code, so that an implementation can be checked against it and not the other way around.

## 1. Why max-plus at all

A task starts when **all** of its predecessors are finished. That is a maximum. Then it runs for its duration. That is an addition. Systems whose dynamics consist only of maximum and addition are linear in the max-plus algebra, even though they are highly nonlinear in ordinary algebra.

The max-plus algebra replaces the usual operations:

| Classic | Max-plus | Meaning here |
|---|---|---|
| `a + b` | `a ⊕ b = max(a, b)` | Waiting on the slowest predecessor |
| `a · b` | `a ⊗ b = a + b` | Appending a duration to a start time |
| `0` (identity for +) | `ε = -∞` | Edge does not exist |
| `1` (identity for ·) | `e = 0` | No delay |

If task i in run k starts at time `x_i(k)`, then

```
x(k) = A ⊗ x(k-1)      so      x_i(k) = max_j ( A[i][j] + x_j(k-1) )
```

This is a linear recursion. Just as a classic linear recursion has an eigenvalue that governs the asymptotic growth, this one has a max-plus eigenvalue λ that governs the asymptotic **cycle time**.

## 2. What λ means

λ satisfies `A ⊗ v = λ ⊗ v` for an eigenvector v. In plain terms: after a settling phase, each run shifts by exactly λ relative to the previous one. λ is the cycle time the system can hold under its own power.

The central theorem (Cuninghame-Green): **λ equals the maximum cycle mean of the graph.**

```
λ = max over all cycles C in G:  ( sum of edge weights in C ) / ( number of edges in C )
```

The cycle that realizes this maximum is the **critical cycle**. It is the bottleneck. Any shortening of a task that does not lie on the critical cycle changes λ by exactly zero.

The formula above holds as long as every cross-run edge reaches back exactly one period. An edge with offset *n* (`execution_delta = n · period`) counts as *n* edges, so the denominator is in general the sum of the offsets:

```
λ = max over all cycles C:  ( sum of edge weights in C ) / ( sum of the periods in C )
```

Derivation and consequence for the implementation in [decisions.md](decisions.md), ADR-006.

## 3. Consequence for the schedule

If the pipeline runs with schedule period T:

- **T ≥ λ**: stable. Delays from one run fade out.
- **T < λ**: unstable. Each run starts `λ - T` later than the previous one. The delay grows linearly and without bound.

The drift is thus exactly `λ - T` per run. Not an approximation, but the asymptotic limit. More workers do not change λ, because λ is a property of the dependency structure and not of the capacity. That is the whole point of the tool: it separates capacity problems (solvable by workers) from structural problems (not solvable by workers).

## 4. Condensation: from the task graph to the cross-run matrix

The full DAG has many tasks, but only a few have an edge into the next run. The matrix A is therefore not spanned over all tasks, but only over the **cross-run nodes**, i.e. tasks that have an edge into k+1.

For two cross-run nodes `source` and `target`,

```
Abar[target][source] = longest path in the intra-run DAG
                       from the entry point that the cross-run edge out of `source` feeds,
                       up to and including `target`
```

If no such path exists, the entry is `ε = -∞`. The longest path is well-defined, because the intra-run graph is acyclic. It is computed via topological sort in linear time, not by enumeration.

This condensation is why the method stays fast even for DAGs with hundreds of tasks: the eigenvalue computation runs on a matrix on the order of the cross-run nodes, typically single digit to low double digit.

## 5. Karp

Karp's algorithm computes the maximum cycle mean exactly in `O(V · E)`:

```
D[0][v] = 0 for a start node s, else -∞
D[k][v] = max over edges (u → v):  D[k-1][u] + w(u, v)      for k = 1..n

λ = max over v with D[n][v] > -∞:
      min over k = 0..n-1 with D[k][v] > -∞:
        ( D[n][v] - D[k][v] ) / ( n - k )
```

Karp yields λ reliably, but **not** the critical cycle. That has to be reconstructed separately.

## 6. Critical cycle: Howard, not enumeration

The naive search for the critical cycle tests all cycles. That is no longer feasible beyond about twelve nodes, because the number of cycles grows factorially.

**Howard's policy iteration** is the right answer. It is nearly linear in practice and yields the critical cycle directly as a by-product:

1. Pick an arbitrary outgoing edge for each node. That is the policy π. The graph made of all policy edges has exactly one cycle per component, because each node has out-degree one.
2. For the current policy, compute the cycle mean η and the bias values v (potentials relative to the cycle).
3. Look for an edge `(u → w)` that pays off: `w(u, w) + v[w] - η > v[u]`. If there is none, the policy is optimal and η = λ.
4. Switch to this edge and go to step 2.

At termination, the cycle in the final policy is the critical cycle. Howard is thus both faster and more informative than Karp. Karp nonetheless stays in the code, as an independent second opinion: both have to yield the same λ, and a test pins that. Two independent methods that agree are the best available evidence of correctness, as long as no external reference exists.

## 7. Stochastics

Task durations are not constants. For each task a lognormal fit is computed (lognormal, because run durations are positive and right-skewed), analytically from the available aggregates: `mu = ln(p50)`, `sigma = (ln(p95) − ln(p50)) / 1.6449` (z-value of the 95th percentile) — no raw data is needed for that. Tasks without reliable spread (n < 5, in particular `assume` values) enter the sampling as a constant; an invented variance would be an invented p95. Monte Carlo over these distributions yields a distribution of λ, from which `λ_p50` and `λ_p95` are read off. Important: the condensation runs anew **per sample**, because the edge weights of the condensed matrix are longest paths, and with different durations a different path can be the longest (implementation `eigenlag/montecarlo.py`, session 009).

`λ_p95` is the number that actually matters: it answers whether the schedule holds on a bad day too. A schedule that is stable against `λ_p50` and not against `λ_p95` is a pipeline that runs off the rails once a month and that nobody can explain.

## 8. Limits of the model

Named honestly, so that nobody reads more into it than is there:

- **Unbounded parallelism assumed.** λ is a lower bound. With too few workers the real cycle time is larger. The tool says "no faster than λ", not "λ is achievable".
- **Deterministic durations in the core.** The stochastics sit around the outside as Monte Carlo, not inside the max-plus computation itself.
- **Retries, sensor poking and pool limits** are not modeled. They can only raise the real cycle time, never lower it, so λ remains a valid lower bound. `max_active_runs=1` was in this list too until session 005; it is now modeled as an edge, because it serializes the runs and thereby often provides the binding edge (ADR-016). λ becomes sharper because of it, the lower-bound property remains.
- **No cross-run edge means no λ.** A DAG without recurrence has no cycle across the time axis. The result is then not "λ = 0", but "not applicable". The difference belongs cleanly in the output, otherwise someone reads a zero as an all-clear.

## 9. The limit the Wikimedia case revealed: tasks that wait on the clock

The model assumes that task durations are **independent of the start time**. A sensor that waits on the current hour's data violates exactly that.

`wdqs_streaming_updater_reconcile_hourly` waits on the Hive partitions of the hour it is running for. If it starts on time, it waits for data that does not exist yet. If it starts 50 minutes late, the data has long been there, and it is through in two minutes. Measured: **correlation between start delay and run duration = −0.504** over 397 runs (`wikimedia/case.md`, section 4).

Formally this is not processing time, but a **release condition**: the task cannot end before `dataTime(k)`, and `dataTime(k)` grows with the wall clock, so by exactly T per run. In the recursion

```
End(k) ≥ max( End(k−1) + Work ,  dataTime(k) + Work )
```

the second term **resets** the delay as soon as it has grown large enough. The cycle is broken, and the pipeline is stable as long as the pure **Work** stays below T, even when the measured run duration lies above T.

**Consequences that belong in the product:**

1. A run duration above the schedule is **no** proof of drift. Whoever merely holds runtime against the schedule produces false alarms. At Wikimedia that would be 29 of 30 DAGs (`wikimedia/case.md`, section 6).
2. Where λ is close to T, this feedback decides between stable and drifting, and it cannot be resolved from the runtime metric alone: the measured durations **are already the result** of the settled state. That is circular, and one has to know it.
3. The visible price of a pipeline at its cycle limit is not a growing but a **constant** delay. For wdqs it is 48 minutes, every hour anew.
