"""CLI (Spec 009): `eigenlag analyze PFAD` — die Schale um die Kette aus Session 008.

argparse statt click/typer (Regel 10, keine schweren Dependencies). Exit-Codes sind
Vertrag: 0 = analysiert (auch instabil — das Urteil ist Sache des Nutzers, das Gate
kommt in 010), 1 = Bedienfehler, 2 = Pfad geparst, aber kein analysierbarer DAG.

Quellen-Mischung wie in 008: DB oder REST liefert, was sie hat, --assume-duration
fuellt Luecken je Task mit Warnung. Ohne jede Quelle bricht die CLI mit Erklaerung
ab, kein stiller Default.

argparse-Hilfe, Fehlermeldungen und Quellen-Beschriftungen sind englisch (ADR-023,
Spec 011): sie tragen keinen --lang-Kontext, weil der Flag erst geparst wird, und
gehen sprachneutral ins --json.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from eigenlag import gate, montecarlo
from eigenlag.analyze import analyze_result
from eigenlag.durations import Statistic, Stats, TaskStats, assume
from eigenlag.messages import Lang
from eigenlag.parse_airflow import ParsedDag, ParseResult, parse_path, select_dags
from eigenlag.report import (
    WhatIfDropEdge,
    WhatIfTask,
    compose,
    render,
    resolve_task_name,
)

OK, BEDIENFEHLER, KEIN_DAG, GATE_AUSGELOEST = 0, 1, 2, 3


class Bedienfehler(Exception):
    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eigenlag",
        description=(
            "Recurrence analyzer for data pipelines: computes the sustainable minimum"
            " cycle time (max-plus eigenvalue λ) and the critical cycle."
        ),
    )
    sub = parser.add_subparsers(dest="befehl", required=True)
    analyze = sub.add_parser("analyze", help="analyze DAG files and print a report")
    analyze.add_argument("pfad", help="DAG file or directory")
    analyze.add_argument("--db", help="SQLAlchemy URL of the Airflow metadata DB")
    analyze.add_argument("--rest", help="base URL of the Airflow REST API")
    analyze.add_argument("--rest-token", help="bearer token for the REST API")
    analyze.add_argument(
        "--assume-duration",
        type=float,
        help="seconds per task without a measurement (fills gaps, with a warning in the report)",
    )
    analyze.add_argument("--dag-id", help="analyze only this DAG (default: all in the path)")
    analyze.add_argument(
        "--statistic",
        choices=["mean", "p50", "p95"],
        default="mean",
        help="which duration statistic feeds λ (default mean, justified in the report)",
    )
    analyze.add_argument("--since", type=int, default=90, help="window for the metadata DB in days")
    analyze.add_argument(
        "--period",
        type=float,
        help="period override in seconds when the schedule is unknown or dataset-triggered",
    )
    analyze.add_argument(
        "--samples", type=int, default=1000, help="Monte Carlo samples, 0 disables it"
    )
    analyze.add_argument(
        "--what-if",
        action="append",
        default=[],
        metavar="task=NAME:SECONDS | drop-edge=SRC->DST",
        help="compute a scenario, repeatable",
    )
    analyze.add_argument(
        "--json", action="store_true", help="machine-readable output instead of text"
    )
    analyze.add_argument(
        "--lang",
        choices=["en", "de"],
        default="en",
        help="report language (default en; --json stays language-independent)",
    )

    demo = sub.add_parser(
        "demo",
        help="render the report of a built-in example pipeline (no files, no network)",
    )
    demo.add_argument(
        "--lang",
        choices=["en", "de"],
        default="en",
        help="report language (default en)",
    )

    check = sub.add_parser(
        "check",
        help="CI gate: compare λ and cross-run edges against a git state",
    )
    check.add_argument("pfad", help="DAG file or directory in the working repo (the after state)")
    check.add_argument(
        "--against",
        required=True,
        metavar="REF",
        help="git reference of the before state (e.g. origin/main, HEAD~1, a tag)",
    )
    check.add_argument("--db", help="SQLAlchemy URL of the Airflow metadata DB")
    check.add_argument(
        "--assume-duration",
        type=float,
        help="seconds per task without a measurement; without a source the structural mode runs",
    )
    check.add_argument("--dag-id", help="compare only this DAG (default: all in the path)")
    check.add_argument(
        "--statistic",
        choices=["mean", "p50", "p95"],
        default="mean",
        help="which duration statistic feeds λ (point λ, ADR-022)",
    )
    check.add_argument("--since", type=int, default=90, help="window for the metadata DB in days")
    check.add_argument(
        "--period",
        type=float,
        help="period override in seconds when the schedule is unknown or dataset-triggered",
    )
    check.add_argument(
        "--fail-on-new-edge",
        action="store_true",
        help="every new cross-run edge triggers, independent of the period",
    )
    check.add_argument(
        "--max-increase",
        type=float,
        metavar="PERCENT",
        help="cap λ growth against REF, even without a new edge",
    )
    check.add_argument(
        "--comment-file", metavar="PATH", help="also write the PR comment (Markdown) to this file"
    )
    check.add_argument(
        "--json", action="store_true", help="machine-readable output instead of text"
    )
    check.add_argument(
        "--lang",
        choices=["en", "de"],
        default="en",
        help="comment language (default en; --json stays language-independent)",
    )
    check.set_defaults(rest=None, rest_token=None)  # _fetch_stats teilt sich analyze und check
    return parser


def _parse_what_if(text: str) -> WhatIfTask | WhatIfDropEdge:
    if text.startswith("task="):
        name, sep, seconds = text[len("task=") :].rpartition(":")
        if not sep or not name:
            raise Bedienfehler(f"--what-if {text!r}: expected task=NAME:SECONDS")
        try:
            return WhatIfTask(task=name, seconds=float(seconds))
        except ValueError as err:
            raise Bedienfehler(f"--what-if {text!r}: {seconds!r} is not a number") from err
    if text.startswith("drop-edge="):
        src, sep, dst = text[len("drop-edge=") :].partition("->")
        if not sep or not src or not dst:
            raise Bedienfehler(f"--what-if {text!r}: expected drop-edge=SRC->DST")
        return WhatIfDropEdge(src=src.strip(), dst=dst.strip())
    raise Bedienfehler(f"--what-if {text!r}: expected task=NAME:SECONDS or drop-edge=SRC->DST")


def _redact(url: str) -> str:
    return re.sub(r"://([^:/@]+):[^@]+@", r"://\1:***@", url)


def _fetch_stats(args: argparse.Namespace, dag_ids: Sequence[str]) -> tuple[Stats, str]:
    """Dauern-Quelle aufloesen: (Stats, englische Beschreibung fuer den Report-Kopf)."""
    if args.db and args.rest:
        raise Bedienfehler("--db and --rest are mutually exclusive: choose one source")
    if args.rest and not args.rest_token:
        raise Bedienfehler("--rest needs --rest-token (Airflow 3: JWT via POST /auth/token)")
    if args.rest_token and not args.rest:
        raise Bedienfehler("--rest-token without --rest makes no sense")
    if not args.db and not args.rest and args.assume_duration is None:
        raise Bedienfehler(
            "no duration source: pass --db URL (Airflow metadata DB), --rest URL with"
            " --rest-token (Airflow REST API) or --assume-duration SECONDS."
            " Without durations there is no time statement, and nothing is guessed."
        )

    quellen: list[str] = []
    stats: Stats = {}
    if args.db:
        from eigenlag.durations import from_metadata_db

        stats, _ = from_metadata_db(args.db, list(dag_ids), since_days=args.since)
        quellen.append(f"Airflow metadata DB {_redact(args.db)}, window {args.since} days")
    elif args.rest:
        from eigenlag.durations import from_rest

        stats, _ = from_rest(args.rest, args.rest_token, list(dag_ids), since_days=args.since)
        quellen.append(f"Airflow REST {args.rest}, window {args.since} days")
    if args.assume_duration is not None:
        quellen.append(f"assumed: {args.assume_duration:g} s per task without a measurement")
    return stats, " + ".join(quellen)


def _takt(dags: Sequence[ParsedDag], period: float | None) -> tuple[float | None, str | None]:
    if period is not None:
        return period, f"--period {period:g}"
    perioden = {dag.period_s for dag in dags if dag.period_s is not None}
    if len(perioden) != 1:
        return None, None  # kein oder kein gemeinsamer Takt: kein Urteil, kein Raten
    ausdruecke = sorted({dag.schedule_expr or "" for dag in dags if dag.period_s is not None})
    return perioden.pop(), f"schedule {', '.join(ausdruecke)}"


def _run_analyze(args: argparse.Namespace) -> int:
    root = Path(args.pfad)
    if not root.exists():
        raise Bedienfehler(f"path {args.pfad!r} does not exist")

    requested = [_parse_what_if(text) for text in args.what_if]

    result = parse_path(root)
    dags = result.dags
    if args.dag_id is not None:
        dags = select_dags(result, args.dag_id)
        if not dags:
            gefunden = ", ".join(sorted({d.dag_id or "(no dag_id)" for d in result.dags}))
            print(
                f"no analyzable DAG: --dag-id {args.dag_id!r} not found."
                f" Found in the path: {gefunden or 'no DAGs'}",
                file=sys.stderr,
            )
            return KEIN_DAG
        files = {dag.file for dag in dags}
        result = ParseResult(
            dags=dags, warnings=tuple(w for w in result.warnings if w.file in files)
        )
    if not any(dag.tasks for dag in dags):
        print(
            f"no analyzable DAG in {args.pfad!r}"
            f" ({len(dags)} DAG definitions, none with statically resolvable tasks).",
            file=sys.stderr,
        )
        for warning in result.warnings + tuple(w for d in dags for w in d.warnings):
            print(
                f"  {warning.file}:{warning.lineno} {warning.kind} {warning.detail}",
                file=sys.stderr,
            )
        return KEIN_DAG

    dag_ids = [dag.dag_id for dag in dags if dag.dag_id is not None]
    stats, dauern_quelle = _fetch_stats(args, dag_ids)
    fallback = assume(args.assume_duration) if args.assume_duration is not None else None
    statistic = cast(Statistic, args.statistic)

    try:
        analysis = analyze_result(result, stats, statistic, fallback)
    except ValueError as err:
        raise Bedienfehler(
            f"{err} — fill gaps with --assume-duration SECONDS, per task with a warning."
        ) from err

    takt_s, takt_quelle = _takt(dags, args.period)

    resolved: list[WhatIfTask | WhatIfDropEdge] = []
    for wish in requested:
        if isinstance(wish, WhatIfTask):
            resolved.append(
                WhatIfTask(resolve_task_name(analysis.pipeline, wish.task), wish.seconds)
            )
        else:
            resolved.append(
                WhatIfDropEdge(
                    resolve_task_name(analysis.pipeline, wish.src),
                    resolve_task_name(analysis.pipeline, wish.dst),
                )
            )

    mc = None
    if args.samples > 0:
        mc = montecarlo.run(analysis.pipeline, stats, samples=args.samples, period=takt_s)

    bericht = compose(
        pfad=args.pfad,
        dags=dags,
        analysis=analysis,
        stats=stats,
        statistic=statistic,
        takt_s=takt_s,
        takt_quelle=takt_quelle,
        dauern_quelle=dauern_quelle,
        monte_carlo=mc,
        requested=resolved,
    )
    if args.json:
        print(json.dumps(bericht, ensure_ascii=False, indent=2))
    else:
        print(render(bericht, cast(Lang, args.lang)))
    return OK


def _run_check(args: argparse.Namespace) -> int:
    pfad = Path(args.pfad)
    if not pfad.exists():
        raise Bedienfehler(f"path {args.pfad!r} does not exist")

    try:
        root = gate.repo_root(pfad)
    except gate.GitFehler as err:
        raise Bedienfehler(f"path {args.pfad!r} is not inside a git repo: {err}") from err
    try:
        ref_sha = gate.resolve_ref(root, args.against)
    except gate.GitFehler as err:
        raise Bedienfehler(f"--against {args.against!r} is not resolvable: {err}") from err
    rel = pfad.resolve().relative_to(root.resolve())

    after = parse_path(pfad)
    with gate.worktree(root, ref_sha) as vergleich:
        vorher_pfad = vergleich / rel
        before = (
            parse_path(vorher_pfad) if vorher_pfad.exists() else ParseResult(dags=(), warnings=())
        )

    if args.dag_id is not None:
        ids = {dag.dag_id for dag in (*before.dags, *after.dags) if dag.dag_id is not None}
        if args.dag_id not in ids:
            gefunden = ", ".join(sorted(ids)) or "no DAGs"
            raise Bedienfehler(
                f"--dag-id {args.dag_id!r} not found in either state. Found: {gefunden}"
            )

    struct_mode = args.db is None and args.assume_duration is None
    if struct_mode:
        stats: Stats = {}
        fallback: TaskStats | None = assume(1.0)
        dauern_quelle = "structural mode: uniform duration 1.0 per task"
    else:
        dag_ids = sorted(
            {dag.dag_id for dag in (*before.dags, *after.dags) if dag.dag_id is not None}
        )
        stats, dauern_quelle = _fetch_stats(args, dag_ids)
        fallback = assume(args.assume_duration) if args.assume_duration is not None else None

    try:
        ergebnis = gate.compose_check(
            pfad=args.pfad,
            ref=args.against,
            before=before,
            after=after,
            stats=stats,
            statistic=cast(Statistic, args.statistic),
            fallback=fallback,
            struct_mode=struct_mode,
            dauern_quelle=dauern_quelle,
            period_override=args.period,
            dag_id_filter=args.dag_id,
            fail_on_new_edge=args.fail_on_new_edge,
            max_increase=args.max_increase,
        )
    except ValueError as err:
        raise Bedienfehler(
            f"{err} — fill gaps with --assume-duration SECONDS, per task with a warning."
        ) from err

    kommentar = gate.render_check(ergebnis, cast(Lang, args.lang))
    if args.comment_file:
        Path(args.comment_file).write_text(kommentar, encoding="utf-8")
    if args.json:
        print(json.dumps(ergebnis, ensure_ascii=False, indent=2))
    else:
        print(kommentar)
    return int(ergebnis["exit_code"])


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.befehl == "check":
            return _run_check(args)
        if args.befehl == "demo":
            from eigenlag.demo import demo_text

            print(demo_text(cast(Lang, args.lang)))
            return OK
        return _run_analyze(args)
    except (Bedienfehler, ValueError, gate.GitFehler) as err:
        # ValueError: Systemgrenze User-Input (unbekannter What-if-Task, kaputte Kante).
        # GitFehler: Systemgrenze git (Worktree-Anlage schlug fehl).
        print(f"error: {err}", file=sys.stderr)
        return BEDIENFEHLER


if __name__ == "__main__":
    sys.exit(main())
