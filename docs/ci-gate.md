# CI gate: `eigenlag check --against REF`

The gate compares λ and the cross-run edge set of the working tree against a git
state (`REF`, typically `origin/main`) and fires before a change that lifts the
pipeline over its cycle limit is merged.

```
eigenlag check PATH --against REF
  [--db URL | --assume-duration SEC]     otherwise structural mode
  [--dag-id ID] [--period SEC] [--statistic mean|p50|p95] [--since DAYS]
  [--fail-on-new-edge] [--max-increase PERCENT]
  [--comment-file PATH] [--json] [--lang en|de]
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | passed — also: no DAGs in both states (with a note) |
| 1 | usage error (path missing, REF not resolvable, no git repo, unknown `--dag-id`) |
| 3 | gate triggered |

Exit 2 stays reserved for `analyze`; the spaces do not overlap.

## Fail rules

**Default:** exit 3 when a new cross-run edge was added **and** λ after lies above the
period T (T from the schedule or `--period`). With `--db`/`--assume-duration` λ is in
seconds and the comparison is literal. Without a duration source (structural mode, the
CI default) λ is in task units and not comparable to seconds; then a new edge that
closes a cycle across the time axis at a known sub-daily schedule triggers (ADR-022).

**Stricter modes:**

- `--fail-on-new-edge`: every new cross-run edge triggers, independent of T, for teams
  that budget edges deliberately.
- `--max-increase PERCENT`: caps λ growth against `REF`, even without a new edge (for
  instance when a task moves onto the critical cycle).

The gate metric is point λ against point λ on the same statistic (ADR-022). Monte
Carlo never runs against thresholds.

## Mechanics

The comparison state comes from a temporary detached worktree
(`git worktree add --detach`), the same relative path is parsed on both states, and
the worktree is removed afterwards, including on error. No checkout in the user's tree,
no mutation of the working repo, no network. The PR comment goes to stdout, or with
`--comment-file` to a file; **the tool never posts anything itself** — posting is the
CI job's business.

## GitHub Actions example

```yaml
name: eigenlag-gate

on:
  pull_request:
    paths:
      - "dags/**"

jobs:
  check:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # the gate needs the base reference in the clone

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: install eigenlag
        run: pip install eigenlag  # or: pip install git+<repo-url>

      - name: run the gate
        id: gate
        run: |
          eigenlag check dags --against "origin/${{ github.base_ref }}" \
            --comment-file comment.md

      - name: post the PR comment
        if: always() && steps.gate.outcome != 'skipped'
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          path: comment.md
```

The `check` step aborts the job on exit 3; the comment step runs anyway thanks to
`if: always()` and posts the text from `comment.md`. With a metadata DB reachable from
CI the structural comparison becomes a comparison in seconds: append
`--db "$AIRFLOW_DB_URL"`.
