"""CLI (Spec 009): `eigenlag analyze PFAD` — die Schale um die Kette aus Session 008.

argparse statt click/typer (Regel 10, keine schweren Dependencies). Exit-Codes sind
Vertrag: 0 = analysiert (auch instabil — das Urteil ist Sache des Nutzers, das Gate
kommt in 010), 1 = Bedienfehler, 2 = Pfad geparst, aber kein analysierbarer DAG.

Quellen-Mischung wie in 008: DB oder REST liefert, was sie hat, --assume-duration
fuellt Luecken je Task mit Warnung. Ohne jede Quelle bricht die CLI mit Erklaerung
ab, kein stiller Default.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from eigenlag import montecarlo
from eigenlag.analyze import analyze_result
from eigenlag.durations import Statistic, Stats, assume
from eigenlag.parse_airflow import ParsedDag, ParseResult, parse_path
from eigenlag.report import (
    WhatIfDropEdge,
    WhatIfTask,
    compose,
    render,
    resolve_task_name,
)

OK, BEDIENFEHLER, KEIN_DAG = 0, 1, 2


class Bedienfehler(Exception):
    pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eigenlag",
        description=(
            "Rekurrenz-Analyzer fuer Daten-Pipelines: berechnet die nachhaltige"
            " minimale Taktzeit (Max-Plus-Eigenwert Lambda) und den kritischen Kreis."
        ),
    )
    sub = parser.add_subparsers(dest="befehl", required=True)
    analyze = sub.add_parser("analyze", help="DAG-Files analysieren und Report ausgeben")
    analyze.add_argument("pfad", help="DAG-File oder Verzeichnis")
    analyze.add_argument("--db", help="SQLAlchemy-URL der Airflow-Metadaten-DB")
    analyze.add_argument("--rest", help="Basis-URL der Airflow-REST-API")
    analyze.add_argument("--rest-token", help="Bearer-Token fuer die REST-API")
    analyze.add_argument(
        "--assume-duration",
        type=float,
        help="Sekunden je Task ohne Messung (fuellt Luecken, mit Warnung im Report)",
    )
    analyze.add_argument("--dag-id", help="nur diesen DAG analysieren (sonst: alle im Pfad)")
    analyze.add_argument(
        "--statistic",
        choices=["mean", "p50", "p95"],
        default="mean",
        help="welche Dauer-Statistik in Lambda eingeht (Default mean, im Report begruendet)",
    )
    analyze.add_argument(
        "--since", type=int, default=90, help="Fenster fuer die Metadaten-DB in Tagen"
    )
    analyze.add_argument(
        "--period",
        type=float,
        help="Takt-Override in Sekunden, wenn der Schedule unbekannt oder dataset-getriggert ist",
    )
    analyze.add_argument(
        "--samples", type=int, default=1000, help="Monte-Carlo-Stichproben, 0 schaltet ab"
    )
    analyze.add_argument(
        "--what-if",
        action="append",
        default=[],
        metavar="task=NAME:SEKUNDEN | drop-edge=SRC->DST",
        help="Szenario rechnen, wiederholbar",
    )
    analyze.add_argument("--json", action="store_true", help="maschinenlesbare Ausgabe statt Text")
    return parser


def _parse_what_if(text: str) -> WhatIfTask | WhatIfDropEdge:
    if text.startswith("task="):
        name, sep, seconds = text[len("task=") :].rpartition(":")
        if not sep or not name:
            raise Bedienfehler(f"--what-if {text!r}: erwartet task=NAME:SEKUNDEN")
        try:
            return WhatIfTask(task=name, seconds=float(seconds))
        except ValueError as err:
            raise Bedienfehler(f"--what-if {text!r}: {seconds!r} ist keine Zahl") from err
    if text.startswith("drop-edge="):
        src, sep, dst = text[len("drop-edge=") :].partition("->")
        if not sep or not src or not dst:
            raise Bedienfehler(f"--what-if {text!r}: erwartet drop-edge=SRC->DST")
        return WhatIfDropEdge(src=src.strip(), dst=dst.strip())
    raise Bedienfehler(f"--what-if {text!r}: erwartet task=NAME:SEKUNDEN oder drop-edge=SRC->DST")


def _select_dags(result: ParseResult, dag_id: str) -> tuple[ParsedDag, ...]:
    """Der gewaehlte DAG plus transitiv alle DAGs, auf die seine Sensor-Kanten zeigen —
    ohne sie liesse sich die Pipeline nicht bauen (die Kanten-Quelle waere unbekannt)."""
    selected = [dag for dag in result.dags if dag.dag_id == dag_id]
    if not selected:
        return ()
    by_id = {dag.dag_id: dag for dag in result.dags if dag.dag_id is not None}
    while True:
        missing = [
            other
            for edge in (e for dag in selected for e in dag.cross)
            if edge.signal == "external_task_sensor"
            for other_id, other in by_id.items()
            if other not in selected and edge.src.startswith(f"{other_id}.")
        ]
        if not missing:
            return tuple(selected)
        selected.extend(dict.fromkeys(missing))


def _redact(url: str) -> str:
    return re.sub(r"://([^:/@]+):[^@]+@", r"://\1:***@", url)


def _fetch_stats(args: argparse.Namespace, dag_ids: Sequence[str]) -> tuple[Stats, str]:
    """Dauern-Quelle aufloesen: (Stats, Beschreibung fuer den Report-Kopf)."""
    if args.db and args.rest:
        raise Bedienfehler("--db und --rest schliessen sich aus: eine Quelle waehlen")
    if args.rest and not args.rest_token:
        raise Bedienfehler("--rest braucht --rest-token (Airflow 3: JWT via POST /auth/token)")
    if args.rest_token and not args.rest:
        raise Bedienfehler("--rest-token ohne --rest ergibt keinen Sinn")
    if not args.db and not args.rest and args.assume_duration is None:
        raise Bedienfehler(
            "keine Dauern-Quelle: --db URL (Airflow-Metadaten-DB), --rest URL mit"
            " --rest-token (Airflow-REST-API) oder --assume-duration SEKUNDEN angeben."
            " Ohne Dauern gibt es keine Zeit-Aussage, und geraten wird nicht."
        )

    quellen: list[str] = []
    stats: Stats = {}
    if args.db:
        from eigenlag.durations import from_metadata_db

        stats, _ = from_metadata_db(args.db, list(dag_ids), since_days=args.since)
        quellen.append(f"Metadaten-DB {_redact(args.db)}, Fenster {args.since} Tage")
    elif args.rest:
        from eigenlag.durations import from_rest

        stats, _ = from_rest(args.rest, args.rest_token, list(dag_ids), since_days=args.since)
        quellen.append(f"Airflow-REST {args.rest}, Fenster {args.since} Tage")
    if args.assume_duration is not None:
        quellen.append(f"angenommen: {args.assume_duration:g} s je Task ohne Messung")
    return stats, " + ".join(quellen)


def _takt(dags: Sequence[ParsedDag], period: float | None) -> tuple[float | None, str | None]:
    if period is not None:
        return period, f"--period {period:g}"
    perioden = {dag.period_s for dag in dags if dag.period_s is not None}
    if len(perioden) != 1:
        return None, None  # kein oder kein gemeinsamer Takt: kein Urteil, kein Raten
    ausdruecke = sorted({dag.schedule_expr or "" for dag in dags if dag.period_s is not None})
    return perioden.pop(), f"Schedule {', '.join(ausdruecke)}"


def _run_analyze(args: argparse.Namespace) -> int:
    root = Path(args.pfad)
    if not root.exists():
        raise Bedienfehler(f"Pfad {args.pfad!r} existiert nicht")

    requested = [_parse_what_if(text) for text in args.what_if]

    result = parse_path(root)
    dags = result.dags
    if args.dag_id is not None:
        dags = _select_dags(result, args.dag_id)
        if not dags:
            gefunden = ", ".join(sorted({d.dag_id or "(ohne dag_id)" for d in result.dags}))
            print(
                f"kein analysierbarer DAG: --dag-id {args.dag_id!r} nicht gefunden."
                f" Im Pfad gefunden: {gefunden or 'keine DAGs'}",
                file=sys.stderr,
            )
            return KEIN_DAG
        files = {dag.file for dag in dags}
        result = ParseResult(
            dags=dags, warnings=tuple(w for w in result.warnings if w.file in files)
        )
    if not any(dag.tasks for dag in dags):
        print(
            f"kein analysierbarer DAG in {args.pfad!r}"
            f" ({len(dags)} DAG-Definitionen, keine mit statisch aufloesbaren Tasks).",
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
            f"{err} — Luecken fuellt --assume-duration SEKUNDEN, je Task mit Warnung."
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
        print(render(bericht))
    return OK


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _run_analyze(args)
    except (Bedienfehler, ValueError) as err:
        # ValueError: Systemgrenze User-Input (unbekannter What-if-Task, kaputte Kante).
        print(f"Bedienfehler: {err}", file=sys.stderr)
        return BEDIENFEHLER


if __name__ == "__main__":
    sys.exit(main())
