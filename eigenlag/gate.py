"""CI-Gate (Spec 010): `eigenlag check --against REF`.

Vergleicht je DAG das Punkt-Lambda und die Cross-Run-Kanten-Menge zweier
Git-Staende: REF (vorher) gegen den Arbeitsstand (nachher). Gate-Metrik ist
Punkt-Lambda gegen Punkt-Lambda (ADR-022): dieselbe Statistik auf beiden
Staenden, deterministisch, jede Differenz einer Code-Aenderung zuordenbar.
Monte Carlo laeuft nie gegen Schwellen und taucht hier nicht einmal auf.

Der Vorher-Stand kommt aus einem temporaeren detached Worktree, nie aus einem
Checkout im Nutzer-Tree: Multi-File-Konstrukte (Factories, Sensor-Ziele)
funktionieren so auf beiden Staenden identisch, und das Arbeits-Repo bleibt
unangetastet. Kein GitHub-API-Call, niemals — der PR-Kommentar geht als
Markdown auf stdout bzw. in --comment-file, das Posten ist Sache des CI-Jobs.

Default-Fail-Regel nach Auftrag: neue Cross-Run-Kante und Lambda_nachher > T.
Ohne Dauern-Quelle (Struktur-Modus, der CI-Default) ist Lambda in
Task-Einheiten nicht gegen T in Sekunden vergleichbar; dann loest eine neue
Kante aus, die einen Kreis ueber die Zeitachse schliesst, bei bekanntem
sub-taeglichem Takt — deckungsgleich mit der Risiko-Definition aus ADR-018.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from eigenlag.analyze import Analysis, analyze_result
from eigenlag.durations import Statistic, TaskStats
from eigenlag.maxplus import TOL
from eigenlag.model import Pipeline
from eigenlag.parse_airflow import (
    ParsedCrossEdge,
    ParsedDag,
    ParseResult,
    node_name,
    select_dags,
)
from eigenlag.report import _cycle_cross_pairs, _dauer, _num, _what_if, cycle_report

SUB_TAEGLICH_S = 86400.0

MODELLGRENZEN_KURZ = (
    "Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist"
    " angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie"
    " koennen die reale Taktzeit nur erhoehen, nie senken."
)

STRUKTUR_HINWEIS = (
    "Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine"
    " Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder"
    " --assume-duration."
)


class GitFehler(Exception):
    """Systemgrenze git: kein Repo, nicht aufloesbare REF, Worktree-Fehler."""


def _git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise GitFehler(proc.stderr.strip() or f"git {' '.join(args)} schlug fehl")
    return proc.stdout.strip()


def repo_root(path: Path) -> Path:
    base = path if path.is_dir() else path.parent
    return Path(_git(base, "rev-parse", "--show-toplevel"))


def resolve_ref(root: Path, ref: str) -> str:
    return _git(root, "rev-parse", "--verify", f"{ref}^{{commit}}")


@contextmanager
def worktree(root: Path, ref: str) -> Iterator[Path]:
    """Temporaerer detached Worktree auf REF, read-only benutzt, danach restlos weg —
    auch wenn im with-Block eine Exception fliegt."""
    tmp = Path(tempfile.mkdtemp(prefix="eigenlag-check-"))
    ort = tmp / "ref"
    try:
        _git(root, "worktree", "add", "--detach", "--quiet", str(ort), ref)
        yield ort
    finally:
        subprocess.run(
            ["git", "-C", str(root), "worktree", "remove", "--force", str(ort)],
            capture_output=True,
            text=True,
            check=False,
        )
        shutil.rmtree(tmp, ignore_errors=True)


# --- Vergleich --------------------------------------------------------------------------


def _edge_key(dag: ParsedDag, edge: ParsedCrossEdge) -> tuple[str, str, int, str]:
    """Kanten-Identitaet ueber Staende hinweg: namespaced Enden, Versatz, Signal-Art.
    Datei und Zeile bleiben draussen — eine verschobene Zeile ist keine neue Kante."""
    src = edge.src if edge.signal == "external_task_sensor" else node_name(dag, edge.src)
    return (src, node_name(dag, edge.dst), edge.periods, edge.signal)


def _closes_cycle(pipeline: Pipeline, src: str, dst: str) -> bool:
    """Liegt die Cross-Kante src(k-n) -> dst(k) auf einem Kreis ueber die Zeitachse?
    Genau dann, wenn dst ueber Intra- und Cross-Kanten zurueck nach src fuehrt."""
    succs: dict[str, list[str]] = {task: [] for task in pipeline.tasks}
    for a, b in pipeline.intra:
        succs[a].append(b)
    for edge in pipeline.cross:
        succs[edge.src].append(edge.dst)
    seen = {dst}
    queue = [dst]
    while queue:
        node = queue.pop()
        if node == src:
            return True
        for nxt in succs[node]:
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return False


def _analysis_for(
    result: ParseResult,
    dag_id: str,
    stats: Mapping[str, TaskStats],
    statistic: Statistic,
    fallback: TaskStats | None,
) -> Analysis:
    subset = ParseResult(dags=select_dags(result, dag_id), warnings=())
    return analyze_result(subset, stats, statistic, fallback)


def _takt(dag: ParsedDag | None, period_override: float | None) -> tuple[float | None, str | None]:
    if period_override is not None:
        return period_override, f"--period {period_override:g}"
    if dag is None or dag.period_s is None:
        return None, None
    return dag.period_s, f"Schedule {dag.schedule_expr}"


def _fix_hint(analysis: Analysis, takt_s: float | None, struct_mode: bool) -> str:
    """Der What-if-Hinweis: die kleinste Standard-Aenderung, die Lambda unter T bringt.

    Unter den Szenarien, die T unterschreiten, gewinnt das mit dem groessten neuen
    Lambda (die am wenigsten invasive Aenderung, die reicht); Kreis-Aufloesungen
    (kein Lambda mehr) nur, wenn kein endliches Szenario existiert."""
    if struct_mode:
        return (
            "Die ausloesende Kante zu entfernen behebt den Fail. Eine Zeit-Aussage"
            " (Lambda gegen T in Sekunden) braucht eine Dauern-Quelle: --db oder"
            " --assume-duration."
        )
    if takt_s is None:
        return "Kein Takt bekannt; --period setzt ihn, sonst gibt es kein Unter-T-Ziel."
    rows = _what_if(analysis, ())
    unter_t = [r for r in rows if r["lambda_s"] is not None and r["lambda_s"] < takt_s]
    if unter_t:
        best = max(unter_t, key=lambda r: float(r["lambda_s"]))
        return (
            f"{best['szenario']} bringt Lambda auf {_dauer(best['lambda_s'])} und damit"
            f" unter T = {_dauer(takt_s)}."
        )
    ohne_kreis = [r for r in rows if r["lambda_s"] is None]
    if ohne_kreis:
        return (
            "Keine einzelne Standard-Aenderung bringt Lambda unter T, aber"
            f" {ohne_kreis[0]['szenario']} loest den Kreis ganz auf (keine Taktgrenze mehr)."
        )
    return (
        "Keine einzelne Standard-Aenderung (Kreis-Task halbiert, Cross-Kante entfernt)"
        " bringt Lambda unter T; der Kreis traegt an mehreren Stellen dasselbe Zyklusmittel."
    )


def _edge_dict(
    key: tuple[str, str, int, str], edge: ParsedCrossEdge, auf_kreis: bool
) -> dict[str, Any]:
    return {
        "src": key[0],
        "dst": key[1],
        "perioden": key[2],
        "signal": key[3],
        "datei": edge.file,
        "zeile": edge.lineno,
        "auf_kreis": auf_kreis,
    }


def _pick_ausloeser(
    neue_kanten: list[dict[str, Any]], analysis: Analysis | None
) -> dict[str, Any] | None:
    """Die Kante, die den Ausschlag gab: bevorzugt eine neue Kante auf dem kritischen
    Kreis, sonst eine, die ueberhaupt einen Kreis schliesst, sonst die erste neue."""
    if not neue_kanten:
        return None
    if analysis is not None:
        kreis_paare = _cycle_cross_pairs(analysis)
        auf_kritischem = [k for k in neue_kanten if (k["src"], k["dst"]) in kreis_paare]
        if auf_kritischem:
            return auf_kritischem[0]
    auf_kreis = [k for k in neue_kanten if k["auf_kreis"]]
    return auf_kreis[0] if auf_kreis else neue_kanten[0]


def _dag_row(
    dag_id: str,
    before: ParseResult,
    after: ParseResult,
    *,
    stats: Mapping[str, TaskStats],
    statistic: Statistic,
    fallback: TaskStats | None,
    struct_mode: bool,
    period_override: float | None,
    fail_on_new_edge: bool,
    max_increase: float | None,
) -> dict[str, Any]:
    b_dag = next((d for d in before.dags if d.dag_id == dag_id), None)
    a_dag = next((d for d in after.dags if d.dag_id == dag_id), None)
    b_analysis = (
        _analysis_for(before, dag_id, stats, statistic, fallback) if b_dag is not None else None
    )
    a_analysis = (
        _analysis_for(after, dag_id, stats, statistic, fallback) if a_dag is not None else None
    )
    lam_b = b_analysis.lam if b_analysis is not None else None
    lam_a = a_analysis.lam if a_analysis is not None else None
    takt_s, takt_quelle = _takt(a_dag if a_dag is not None else b_dag, period_override)

    b_keys = {_edge_key(b_dag, e) for e in b_dag.cross} if b_dag is not None else set()
    a_edges = {_edge_key(a_dag, e): e for e in a_dag.cross} if a_dag is not None else {}
    neue_kanten = [
        _edge_dict(
            key,
            edge,
            a_analysis is not None and _closes_cycle(a_analysis.pipeline, key[0], key[1]),
        )
        for key, edge in a_edges.items()
        if key not in b_keys
    ]
    entfallene = [
        {"src": k[0], "dst": k[1], "perioden": k[2], "signal": k[3]}
        for k in sorted(b_keys - set(a_edges))
    ]

    gruende: list[str] = []
    if neue_kanten and a_dag is not None:
        if struct_mode:
            kreis_kanten = [k for k in neue_kanten if k["auf_kreis"]]
            if kreis_kanten and takt_s is not None and takt_s < SUB_TAEGLICH_S:
                gruende.append(
                    "neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse"
                    f" bei sub-taeglichem Takt (T = {_dauer(takt_s)})"
                )
        elif takt_s is not None and lam_a is not None and lam_a > takt_s:
            gruende.append(
                f"neue Cross-Run-Kante und Lambda = {_dauer(lam_a)} ueber dem"
                f" Takt T = {_dauer(takt_s)}"
            )
    if fail_on_new_edge and neue_kanten:
        anzahl = (
            "1 neue Cross-Run-Kante"
            if len(neue_kanten) == 1
            else (f"{len(neue_kanten)} neue Cross-Run-Kanten")
        )
        gruende.append(f"{anzahl} (--fail-on-new-edge)")
    if max_increase is not None and lam_a is not None:
        if lam_b is None:
            gruende.append(
                f"Lambda-Anstieg ueber {max_increase:g} % (--max-increase):"
                " vorher kein Kreis, nachher eine Taktgrenze"
            )
        elif lam_a > lam_b * (1.0 + max_increase / 100.0) + TOL:
            anstieg = (lam_a - lam_b) / lam_b * 100.0
            gruende.append(
                f"Lambda-Anstieg {_num(anstieg)} % ueber der Schranke"
                f" {max_increase:g} % (--max-increase)"
            )

    ausgeloest = bool(gruende)
    return {
        "dag_id": dag_id,
        "vorher_vorhanden": b_dag is not None,
        "nachher_vorhanden": a_dag is not None,
        "lambda_vorher": lam_b,
        "lambda_nachher": lam_a,
        "takt_s": takt_s,
        "takt_quelle": takt_quelle,
        "neue_kanten": neue_kanten,
        "entfallene_kanten": entfallene,
        "ausgeloest": ausgeloest,
        "gruende": gruende,
        "ausloeser_kante": _pick_ausloeser(neue_kanten, a_analysis) if ausgeloest else None,
        "kritischer_kreis": (
            cycle_report(select_dags(after, dag_id), a_analysis) if a_analysis is not None else None
        ),
        "behebung": (
            _fix_hint(a_analysis, takt_s, struct_mode)
            if ausgeloest and a_analysis is not None
            else None
        ),
    }


def compose_check(
    *,
    pfad: str,
    ref: str,
    before: ParseResult,
    after: ParseResult,
    stats: Mapping[str, TaskStats],
    statistic: Statistic,
    fallback: TaskStats | None,
    struct_mode: bool,
    dauern_quelle: str,
    period_override: float | None = None,
    dag_id_filter: str | None = None,
    fail_on_new_edge: bool = False,
    max_increase: float | None = None,
) -> dict[str, Any]:
    """Baut das Gate-Ergebnis als dict mit stabilen Keys; render_check macht daraus
    den PR-Kommentar. Eine Quelle fuer Text und --json, wie in 009."""
    before_ids = {d.dag_id for d in before.dags if d.dag_id is not None}
    after_ids = {d.dag_id for d in after.dags if d.dag_id is not None}
    hinweise: list[str] = []

    unbenannt = [d for d in (*before.dags, *after.dags) if d.dag_id is None]
    if unbenannt:
        stellen = ", ".join(f"{d.file}:{d.lineno}" for d in unbenannt)
        hinweise.append(
            f"{len(unbenannt)} DAG-Definition(en) ohne statisch aufloesbare dag_id"
            f" sind nicht vergleichbar und bleiben aussen vor: {stellen}"
        )

    # dag_id_filter validiert die CLI (Systemgrenze User-Input), hier wird nur gefiltert.
    ids = sorted(before_ids | after_ids)
    if dag_id_filter is not None:
        ids = [dag_id for dag_id in ids if dag_id == dag_id_filter]
    if not ids:
        hinweise.append("Keine DAGs in beiden Staenden — nichts zu pruefen.")

    rows = [
        _dag_row(
            dag_id,
            before,
            after,
            stats=stats,
            statistic=statistic,
            fallback=fallback,
            struct_mode=struct_mode,
            period_override=period_override,
            fail_on_new_edge=fail_on_new_edge,
            max_increase=max_increase,
        )
        for dag_id in ids
    ]

    ausgeloeste = [r for r in rows if r["ausgeloest"]]
    grund: str | None = None
    if ausgeloeste:
        erster = ausgeloeste[0]
        grund = f"{erster['dag_id']}: {erster['gruende'][0]}"
        if len(ausgeloeste) > 1:
            grund += f" (und {len(ausgeloeste) - 1} weitere DAGs)"

    return {
        "version": 1,
        "pfad": pfad,
        "ref": ref,
        "modus": "struktur" if struct_mode else "sekunden",
        "einheit": "Task-Einheiten" if struct_mode else "s",
        "statistik": statistic,
        "dauern_quelle": dauern_quelle,
        "bestanden": not ausgeloeste,
        "grund": grund,
        "exit_code": 0 if not ausgeloeste else 3,
        "dags": rows,
        "hinweise": hinweise,
        "modellgrenzen": MODELLGRENZEN_KURZ,
    }


# --- PR-Kommentar (Markdown) -------------------------------------------------------------


def _lam_text(value: float | None, struct_mode: bool) -> str:
    if value is None:
        return "kein Kreis"
    if struct_mode:
        return "1 Task-Einheit" if value == 1.0 else f"{_num(value)} Task-Einheiten"
    return _dauer(value)


def _perioden_text(perioden: int) -> str:
    return "1 Periode zurueck" if perioden == 1 else f"{perioden} Perioden zurueck"


def _kanten_zeile(kante: dict[str, Any]) -> str:
    return (
        f"  - `{kante['src']} -> {kante['dst']}`"
        f" ({kante['signal']}, {kante['datei']}:{kante['zeile']},"
        f" {_perioden_text(kante['perioden'])})"
    )


def _dag_abschnitt(row: dict[str, Any], struct_mode: bool) -> list[str]:
    zeilen = ["", f"### {row['dag_id']}", ""]
    vorher = (
        _lam_text(row["lambda_vorher"], struct_mode)
        if row["vorher_vorhanden"]
        else "existierte nicht"
    )
    nachher = (
        _lam_text(row["lambda_nachher"], struct_mode) if row["nachher_vorhanden"] else "geloescht"
    )
    zeilen.append(f"- Lambda: {vorher} -> {nachher} (vorher -> nachher)")
    if row["takt_s"] is not None:
        zeilen.append(f"- Takt T: {_dauer(row['takt_s'])}, Quelle: {row['takt_quelle']}")
    else:
        zeilen.append("- Takt T: unbekannt (kein statischer Schedule; --period setzt ihn)")
    if row["neue_kanten"]:
        zeilen.append(f"- Neue Cross-Run-Kanten ({len(row['neue_kanten'])}):")
        zeilen.extend(_kanten_zeile(k) for k in row["neue_kanten"])
    if row["entfallene_kanten"]:
        weg = ", ".join(f"`{k['src']} -> {k['dst']}`" for k in row["entfallene_kanten"])
        zeilen.append(f"- Entfallene Cross-Run-Kanten: {weg}")
    if row["ausgeloest"]:
        for grund in row["gruende"]:
            zeilen.append(f"- **Ausgeloest:** {grund}")
        if row["ausloeser_kante"] is not None:
            k = row["ausloeser_kante"]
            zeilen.append(
                f"- **Ausloesende Kante:** `{k['src']} -> {k['dst']}`"
                f" ({k['signal']}, {k['datei']}:{k['zeile']})"
            )
    kreis = row["kritischer_kreis"]
    if kreis is not None and (row["ausgeloest"] or row["neue_kanten"]):
        for kante in kreis["kondensiert"]:
            beleg = ""
            if kante["datei"] is not None:
                beleg = f" [{kante['signal']}, {kante['datei']}:{kante['zeile']}]"
            zeilen.append(
                f"- Kritischer Kreis, kondensiert: `{kante['src']} -> {kante['dst']}`,"
                f" Gewicht {_lam_text(kante['gewicht_s'], struct_mode)},"
                f" {_perioden_text(kante['perioden'])}{beleg}"
            )
        zeilen.append(f"- Aufgeloest: {' -> '.join(kreis['aufgeloest'])}")
    if row["behebung"] is not None:
        zeilen.append(f"- Behebung: {row['behebung']}")
    return zeilen


def render_check(d: dict[str, Any]) -> str:
    struct_mode = d["modus"] == "struktur"
    if d["bestanden"]:
        kopf = (
            f"**eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt"
            f" (`{d['pfad']}` gegen `{d['ref']}`)."
        )
    else:
        kopf = f"**eigenlag check: ausgeloest** — {d['grund']}."
    zeilen = [kopf]
    if struct_mode and d["dags"]:
        zeilen += ["", STRUKTUR_HINWEIS]

    betroffen = [
        r
        for r in d["dags"]
        if r["ausgeloest"]
        or r["neue_kanten"]
        or r["entfallene_kanten"]
        or not r["vorher_vorhanden"]
        or not r["nachher_vorhanden"]
    ]
    for row in betroffen:
        zeilen.extend(_dag_abschnitt(row, struct_mode))
    unveraendert = len(d["dags"]) - len(betroffen)
    if unveraendert:
        zeilen += [
            "",
            f"{unveraendert} DAG(s) ohne Aenderung an Cross-Run-Kanten oder Lambda.",
        ]
    for hinweis in d["hinweise"]:
        zeilen += ["", hinweis]
    zeilen += ["", "---", f"_{d['modellgrenzen']}_", ""]
    return "\n".join(zeilen)
