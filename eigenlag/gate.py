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
from eigenlag.messages import Lang, dur, fmt, perioden, scenario_label, t
from eigenlag.model import Pipeline
from eigenlag.parse_airflow import (
    ParsedCrossEdge,
    ParsedDag,
    ParseResult,
    node_name,
    select_dags,
)
from eigenlag.report import _cycle_cross_pairs, _what_if, cycle_report

SUB_TAEGLICH_S = 86400.0

# Deutsche Fassung fuers --json (ADR-023, sprachneutral eingefroren); render_check
# waehlt die Sprache aus dem Katalog.
MODELLGRENZEN_KURZ = (
    "Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist"
    " angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie"
    " koennen die reale Taktzeit nur erhoehen, nie senken."
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


def _fix_hint_code(analysis: Analysis, takt_s: float | None, struct_mode: bool) -> dict[str, Any]:
    """Der What-if-Hinweis als sprachneutraler Code plus Parameter (ADR-023): die
    kleinste Standard-Aenderung, die Lambda unter T bringt.

    Unter den Szenarien, die T unterschreiten, gewinnt das mit dem groessten neuen
    Lambda (die am wenigsten invasive Aenderung, die reicht); Kreis-Aufloesungen
    (kein Lambda mehr) nur, wenn kein endliches Szenario existiert. Die What-if-Zeile
    wandert strukturiert mit, damit render_check ihr Label pro Sprache bauen kann."""
    if struct_mode:
        return {"code": "behebung_struktur"}
    if takt_s is None:
        return {"code": "behebung_kein_takt"}
    rows = _what_if(analysis, ())
    unter_t = [r for r in rows if r["lambda_s"] is not None and r["lambda_s"] < takt_s]
    if unter_t:
        best = max(unter_t, key=lambda r: float(r["lambda_s"]))
        return {"code": "behebung_bestes", "row": best, "lam_s": best["lambda_s"], "takt_s": takt_s}
    ohne_kreis = [r for r in rows if r["lambda_s"] is None]
    if ohne_kreis:
        return {"code": "behebung_aufloesung", "row": ohne_kreis[0]}
    return {"code": "behebung_keine"}


def _behebung_text(code: dict[str, Any], lang: Lang) -> str:
    name = code["code"]
    if name == "behebung_bestes":
        return t(
            lang,
            name,
            szenario=scenario_label(lang, code["row"]),
            lam=dur(code["lam_s"], lang),
            takt=dur(code["takt_s"], lang),
        )
    if name == "behebung_aufloesung":
        return t(lang, name, szenario=scenario_label(lang, code["row"]))
    return t(lang, name)


def _grund_text(code: dict[str, Any], lang: Lang) -> str:
    name = code["code"]
    if name == "grund_struktur_kreis":
        return t(lang, name, takt=dur(code["takt_s"], lang))
    if name == "grund_lambda_ueber_t":
        return t(lang, name, lam=dur(code["lam_s"], lang), takt=dur(code["takt_s"], lang))
    if name == "grund_fail_on_new_edge":
        n = code["n"]
        anzahl = t(lang, "grund_kante_1") if n == 1 else t(lang, "grund_kante_n", n=n)
        return t(lang, name, anzahl=anzahl)
    if name == "grund_max_increase_neu":
        return t(lang, name, schranke=f"{code['schranke']:g}")
    return t(lang, name, anstieg=fmt(code["anstieg"], lang), schranke=f"{code['schranke']:g}")


def _hinweis_text(code: dict[str, Any], lang: Lang) -> str:
    if code["code"] == "hinweis_unbenannt":
        return t(lang, "hinweis_unbenannt", n=code["n"], stellen=code["stellen"])
    return t(lang, code["code"])


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

    # Fail-Gruende als sprachneutrale Codes (ADR-023); die deutsche 'gruende'-Liste ist
    # die --json-Fassung, render_check baut den Text pro Sprache aus den Codes.
    gruende_codes: list[dict[str, Any]] = []
    if neue_kanten and a_dag is not None:
        if struct_mode:
            kreis_kanten = [k for k in neue_kanten if k["auf_kreis"]]
            if kreis_kanten and takt_s is not None and takt_s < SUB_TAEGLICH_S:
                gruende_codes.append({"code": "grund_struktur_kreis", "takt_s": takt_s})
        elif takt_s is not None and lam_a is not None and lam_a > takt_s:
            gruende_codes.append({"code": "grund_lambda_ueber_t", "lam_s": lam_a, "takt_s": takt_s})
    if fail_on_new_edge and neue_kanten:
        gruende_codes.append({"code": "grund_fail_on_new_edge", "n": len(neue_kanten)})
    if max_increase is not None and lam_a is not None:
        if lam_b is None:
            gruende_codes.append({"code": "grund_max_increase_neu", "schranke": max_increase})
        elif lam_a > lam_b * (1.0 + max_increase / 100.0) + TOL:
            anstieg = (lam_a - lam_b) / lam_b * 100.0
            gruende_codes.append(
                {"code": "grund_max_increase", "anstieg": anstieg, "schranke": max_increase}
            )

    ausgeloest = bool(gruende_codes)
    behebung_code = (
        _fix_hint_code(a_analysis, takt_s, struct_mode)
        if ausgeloest and a_analysis is not None
        else None
    )
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
        "gruende": [_grund_text(c, "de") for c in gruende_codes],
        "gruende_codes": gruende_codes,
        "ausloeser_kante": _pick_ausloeser(neue_kanten, a_analysis) if ausgeloest else None,
        "kritischer_kreis": (
            cycle_report(select_dags(after, dag_id), a_analysis) if a_analysis is not None else None
        ),
        "behebung": _behebung_text(behebung_code, "de") if behebung_code is not None else None,
        "behebung_code": behebung_code,
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
    hinweis_codes: list[dict[str, Any]] = []

    unbenannt = [d for d in (*before.dags, *after.dags) if d.dag_id is None]
    if unbenannt:
        stellen = ", ".join(f"{d.file}:{d.lineno}" for d in unbenannt)
        hinweis_codes.append({"code": "hinweis_unbenannt", "n": len(unbenannt), "stellen": stellen})

    # dag_id_filter validiert die CLI (Systemgrenze User-Input), hier wird nur gefiltert.
    ids = sorted(before_ids | after_ids)
    if dag_id_filter is not None:
        ids = [dag_id for dag_id in ids if dag_id == dag_id_filter]
    if not ids:
        hinweis_codes.append({"code": "hinweis_keine_dags"})

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
    grund = _grund_summary(ausgeloeste, "de")

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
        "hinweise": [_hinweis_text(c, "de") for c in hinweis_codes],
        "hinweis_codes": hinweis_codes,
        "modellgrenzen": MODELLGRENZEN_KURZ,
    }


def _grund_summary(ausgeloeste: list[dict[str, Any]], lang: Lang) -> str | None:
    """Die Kopfzeile 'DAG: Grund (und N weitere DAGs)' in der gewaehlten Sprache,
    aus den strukturierten gruende_codes der ausgeloesten Zeilen."""
    if not ausgeloeste:
        return None
    erster = ausgeloeste[0]
    grund = f"{erster['dag_id']}: {_grund_text(erster['gruende_codes'][0], lang)}"
    if len(ausgeloeste) > 1:
        grund += t(lang, "check_grund_suffix", n=len(ausgeloeste) - 1)
    return grund


# --- PR-Kommentar (Markdown) -------------------------------------------------------------


def _lam_text(value: float | None, struct_mode: bool, lang: Lang) -> str:
    if value is None:
        return t(lang, "check_lam_kein_kreis")
    if struct_mode:
        return (
            t(lang, "check_lam_einheit_1")
            if value == 1.0
            else t(lang, "check_lam_einheit_n", n=fmt(value, lang))
        )
    return dur(value, lang)


def _kanten_zeile(kante: dict[str, Any], lang: Lang) -> str:
    return t(
        lang,
        "check_kanten_zeile",
        src=kante["src"],
        dst=kante["dst"],
        signal=kante["signal"],
        datei=kante["datei"],
        zeile=kante["zeile"],
        perioden=perioden(kante["perioden"], lang),
    )


def _dag_abschnitt(row: dict[str, Any], struct_mode: bool, lang: Lang) -> list[str]:
    zeilen = ["", f"### {row['dag_id']}", ""]
    vorher = (
        _lam_text(row["lambda_vorher"], struct_mode, lang)
        if row["vorher_vorhanden"]
        else t(lang, "check_existierte_nicht")
    )
    nachher = (
        _lam_text(row["lambda_nachher"], struct_mode, lang)
        if row["nachher_vorhanden"]
        else t(lang, "check_geloescht")
    )
    zeilen.append(t(lang, "check_abschnitt_lambda", vorher=vorher, nachher=nachher))
    if row["takt_s"] is not None:
        zeilen.append(
            t(
                lang,
                "check_abschnitt_takt",
                dauer=dur(row["takt_s"], lang),
                quelle=row["takt_quelle"],
            )
        )
    else:
        zeilen.append(t(lang, "check_abschnitt_takt_unbekannt"))
    if row["neue_kanten"]:
        zeilen.append(t(lang, "check_abschnitt_neue_kanten", n=len(row["neue_kanten"])))
        zeilen.extend(_kanten_zeile(k, lang) for k in row["neue_kanten"])
    if row["entfallene_kanten"]:
        weg = ", ".join(f"`{k['src']} -> {k['dst']}`" for k in row["entfallene_kanten"])
        zeilen.append(t(lang, "check_abschnitt_entfallene", liste=weg))
    if row["ausgeloest"]:
        for code in row["gruende_codes"]:
            zeilen.append(t(lang, "check_abschnitt_ausgeloest", grund=_grund_text(code, lang)))
        if row["ausloeser_kante"] is not None:
            k = row["ausloeser_kante"]
            zeilen.append(
                t(
                    lang,
                    "check_abschnitt_ausloeser",
                    src=k["src"],
                    dst=k["dst"],
                    signal=k["signal"],
                    datei=k["datei"],
                    zeile=k["zeile"],
                )
            )
    kreis = row["kritischer_kreis"]
    if kreis is not None and (row["ausgeloest"] or row["neue_kanten"]):
        for kante in kreis["kondensiert"]:
            beleg = ""
            if kante["datei"] is not None:
                beleg = f" [{kante['signal']}, {kante['datei']}:{kante['zeile']}]"
            zeilen.append(
                t(
                    lang,
                    "check_abschnitt_kreis",
                    src=kante["src"],
                    dst=kante["dst"],
                    gewicht=_lam_text(kante["gewicht_s"], struct_mode, lang),
                    perioden=perioden(kante["perioden"], lang),
                    beleg=beleg,
                )
            )
        zeilen.append(t(lang, "check_abschnitt_aufgeloest", pfad=" -> ".join(kreis["aufgeloest"])))
    if row["behebung_code"] is not None:
        zeilen.append(
            t(lang, "check_abschnitt_behebung", text=_behebung_text(row["behebung_code"], lang))
        )
    return zeilen


def render_check(d: dict[str, Any], lang: Lang = "en") -> str:
    struct_mode = d["modus"] == "struktur"
    if d["bestanden"]:
        kopf = t(lang, "check_bestanden", pfad=d["pfad"], ref=d["ref"])
    else:
        ausgeloeste = [r for r in d["dags"] if r["ausgeloest"]]
        kopf = t(lang, "check_ausgeloest", grund=_grund_summary(ausgeloeste, lang))
    zeilen = [kopf]
    if struct_mode and d["dags"]:
        zeilen += ["", t(lang, "check_struktur_hinweis")]

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
        zeilen.extend(_dag_abschnitt(row, struct_mode, lang))
    unveraendert = len(d["dags"]) - len(betroffen)
    if unveraendert:
        zeilen += ["", t(lang, "check_unveraendert", n=unveraendert)]
    for code in d["hinweis_codes"]:
        zeilen += ["", _hinweis_text(code, lang)]
    zeilen += [
        "",
        "---",
        t(lang, "check_modellgrenzen_fuss", text=t(lang, "check_modellgrenzen_kurz")),
        "",
    ]
    return "\n".join(zeilen)
