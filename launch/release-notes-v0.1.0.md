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
