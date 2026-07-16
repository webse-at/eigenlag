"""Der Report (Spec 009): Urteil zuerst, Zahlen danach, Grenzen zum Schluss, aber
pflichtig. Er wird von Data Engineers gelesen, die dem Tool noch nicht trauen und
kein Wiki kennen — jede Behauptung traegt ihre Grundlage bei sich.

compose() baut ein dict mit stabilen Keys (ab Session 010 liest das CI-Gate genau
diese Felder), render() macht daraus den deutschen Text. Eine Quelle fuer beides:
Text und --json koennen nicht auseinanderlaufen.

Der Warnblock und die Modellgrenzen-Fusszeile sind nie abschaltbar. Ein Report, der
seine eigenen Grenzen verschweigt, produziert genau die Fehlalarme, die er anderen
vorwirft (wiki/positioning.md, Zwischenbewertung).
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from eigenlag.analyze import Analysis
from eigenlag.durations import Statistic, TaskStats
from eigenlag.maxplus import condense, howard
from eigenlag.messages import CATALOG, Lang, dur, fmt, perioden, scenario_label, t
from eigenlag.model import Pipeline
from eigenlag.montecarlo import MonteCarloResult
from eigenlag.parse_airflow import ParsedDag, node_name
from eigenlag.plan import build_plan

GRENZBAND = 0.10  # |Lambda - T| < 10 % von T heisst "an der Grenze"

MODELLGRENZEN = [
    "Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen"
    " Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'.",
    "Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die"
    " reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig.",
    "Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum"
    " Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan.",
]


@dataclass(frozen=True)
class WhatIfTask:
    task: str
    seconds: float


@dataclass(frozen=True)
class WhatIfDropEdge:
    src: str
    dst: str


def resolve_task_name(pipeline: Pipeline, name: str) -> str:
    """Akzeptiert volle Knoten-Namen (dag.task) und blanke Task-Namen, wenn eindeutig."""
    if name in pipeline.durations:
        return name
    matches = [task for task in pipeline.tasks if task.rsplit(".", 1)[-1] == name]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"Task {name!r} nicht gefunden. Tasks: {', '.join(pipeline.tasks)}")
    raise ValueError(f"Task {name!r} ist mehrdeutig: {', '.join(matches)}")


def _lam_of(pipeline: Pipeline) -> float | None:
    outcome = howard(condense(pipeline)[0])
    return None if outcome is None else outcome[0]


def _with_duration(pipeline: Pipeline, task: str, seconds: float) -> Pipeline:
    durations = dict(pipeline.durations)
    durations[task] = seconds
    return Pipeline(durations=durations, intra=pipeline.intra, cross=pipeline.cross)


def _without_edge(pipeline: Pipeline, src: str, dst: str) -> Pipeline:
    remaining = [e for e in pipeline.cross if not (e.src == src and e.dst == dst)]
    if len(remaining) == len(pipeline.cross):
        vorhanden = ", ".join(f"{e.src} -> {e.dst}" for e in pipeline.cross) or "keine"
        raise ValueError(f"Cross-Kante {src} -> {dst} nicht gefunden. Vorhanden: {vorhanden}")
    return Pipeline(durations=pipeline.durations, intra=pipeline.intra, cross=remaining)


def _cycle_cross_pairs(analysis: Analysis) -> set[tuple[str, str]]:
    """(src, dst) der Cross-Kanten, die den kritischen Kreis bilden.

    Jede kondensierte Kreis-Kante entstand aus genau einer Cross-Kante; deren dst
    ist der erste Knoten des zugehoerigen Task-Pfads (siehe maxplus.condense).
    """
    if analysis.cycle is None:
        return set()
    _, paths = condense(analysis.pipeline)
    return {(edge.src, paths[(edge.src, edge.dst, edge.periods)][0]) for edge in analysis.cycle}


def _what_if(
    analysis: Analysis, requested: Sequence[WhatIfTask | WhatIfDropEdge]
) -> list[dict[str, Any]]:
    """What-if-Ranking. Jede Zeile traegt sprachneutrale Struktur-Felder
    (art/task/wert_s/src/dst), aus denen scenario_label das Label pro Sprache baut;
    das deutsche 'szenario' hier ist die --json-Fassung (ADR-023) und dient dem Gate."""
    pipeline = analysis.pipeline
    base = analysis.lam
    cycle_pairs = _cycle_cross_pairs(analysis)
    scenarios: list[tuple[dict[str, Any], Pipeline]] = []

    if base is not None:
        for task in analysis.cycle_tasks:
            halved = pipeline.durations[task] / 2
            struktur = {
                "art": "task_halbiert",
                "task": task,
                "wert_s": halved,
                "src": None,
                "dst": None,
                "angefragt": False,
                "auf_kreis": True,
            }
            scenarios.append((struktur, _with_duration(pipeline, task, halved)))
        seen: set[tuple[str, str]] = set()
        for edge in pipeline.cross:
            if (edge.src, edge.dst) in seen:
                continue
            seen.add((edge.src, edge.dst))
            struktur = {
                "art": "cross_entfernt",
                "task": None,
                "wert_s": None,
                "src": edge.src,
                "dst": edge.dst,
                "angefragt": False,
                "auf_kreis": (edge.src, edge.dst) in cycle_pairs,
            }
            scenarios.append((struktur, _without_edge(pipeline, edge.src, edge.dst)))

    for wish in requested:
        if isinstance(wish, WhatIfTask):
            task = resolve_task_name(pipeline, wish.task)
            struktur = {
                "art": "task_gesetzt",
                "task": task,
                "wert_s": wish.seconds,
                "src": None,
                "dst": None,
                "angefragt": True,
                "auf_kreis": task in analysis.cycle_tasks,
            }
            scenarios.append((struktur, _with_duration(pipeline, task, wish.seconds)))
        else:
            struktur = {
                "art": "cross_entfernt",
                "task": None,
                "wert_s": None,
                "src": wish.src,
                "dst": wish.dst,
                "angefragt": True,
                "auf_kreis": (wish.src, wish.dst) in cycle_pairs,
            }
            scenarios.append((struktur, _without_edge(pipeline, wish.src, wish.dst)))

    rows = []
    for struktur, variant in scenarios:
        lam = _lam_of(variant)
        row = {
            "szenario": scenario_label("de", struktur),
            "lambda_s": lam,
            "delta_s": None if lam is None or base is None else lam - base,
            **struktur,
        }
        rows.append(row)
    rows.sort(key=lambda r: (r["lambda_s"] is not None, r["lambda_s"] or 0.0))
    return rows


def _provenance(dags: Sequence[ParsedDag]) -> dict[tuple[str, int], tuple[str, str, int]]:
    """Namespaced Kanten-Quelle plus Versatz -> (Signal, Datei, Zeile) der erzeugenden Kante.

    Mehrere Cross-Kanten mit derselben Quelle und demselben Versatz sind moeglich;
    dann steht hier die erste. Beide waeren echte, belegbare Kanten aus demselben DAG.
    """
    found: dict[tuple[str, int], tuple[str, str, int]] = {}
    for dag in dags:
        for edge in dag.cross:
            src = edge.src if edge.signal == "external_task_sensor" else node_name(dag, edge.src)
            found.setdefault((src, edge.periods), (edge.signal, edge.file, edge.lineno))
    return found


def cycle_report(dags: Sequence[ParsedDag], analysis: Analysis) -> dict[str, Any] | None:
    """Der kritische Kreis, kondensiert und aufgeloest (ADR-002), mit Herkunft je Kante.

    Gemeinsame Quelle fuer den analyze-Report und das CI-Gate (Session 010)."""
    if analysis.cycle is None:
        return None
    herkunft = _provenance(dags)
    _, paths = condense(analysis.pipeline)
    kanten = []
    for edge in analysis.cycle:
        signal, datei, zeile = herkunft.get((edge.src, edge.periods), (None, None, None))
        kanten.append(
            {
                "src": edge.src,
                "dst": edge.dst,
                "gewicht_s": edge.weight,
                "perioden": edge.periods,
                "signal": signal,
                "datei": datei,
                "zeile": zeile,
                "task_pfad": list(paths[(edge.src, edge.dst, edge.periods)]),
            }
        )
    return {"kondensiert": kanten, "aufgeloest": list(analysis.cycle_tasks)}


def _urteil(lam: float | None, takt_s: float | None) -> dict[str, Any]:
    if lam is None:
        return {"urteil": "nicht_anwendbar"}
    if takt_s is None:
        return {"urteil": "takt_unbekannt"}
    if abs(lam - takt_s) < GRENZBAND * takt_s:
        return {"urteil": "an_der_grenze"}
    if lam < takt_s:
        return {"urteil": "stabil", "reserve_prozent": (takt_s - lam) / takt_s * 100.0}
    drift = lam - takt_s
    return {
        "urteil": "instabil",
        "drift_s_pro_lauf": drift,
        "laeufe_bis_1h_rueckstand": 3600.0 / drift,
    }


def compose(
    *,
    pfad: str,
    dags: Sequence[ParsedDag],
    analysis: Analysis,
    stats: Mapping[str, TaskStats],
    statistic: Statistic,
    takt_s: float | None,
    takt_quelle: str | None,
    dauern_quelle: str,
    monte_carlo: MonteCarloResult | None,
    requested: Sequence[WhatIfTask | WhatIfDropEdge] = (),
) -> dict[str, Any]:
    pipeline = analysis.pipeline
    ns = [stats[task].n if task in stats else 0 for task in pipeline.tasks]
    kreis = cycle_report(dags, analysis)
    what_if = _what_if(analysis, requested)

    mc: dict[str, Any] | None = None
    if monte_carlo is not None:
        mc = {
            "lambda_p50_s": monte_carlo.lam_p50,
            "lambda_p95_s": monte_carlo.lam_p95,
            "anteil_ueber_takt": monte_carlo.share_above_period,
            "samples": monte_carlo.samples,
            "seed": monte_carlo.seed,
            "konstant_gesampelt": list(monte_carlo.deterministic_tasks),
        }

    warnungen: list[dict[str, Any]] = [
        {"art": w.kind, "task": w.task or None, "datei": None, "zeile": None, "detail": w.detail}
        for w in analysis.warnings
    ]
    warnungen += [
        {"art": w.kind, "task": None, "datei": w.file, "zeile": w.lineno, "detail": w.detail}
        for w in analysis.parse_warnings
    ]

    return {
        "version": 1,
        "pfad": pfad,
        "dags": [
            {
                "dag_id": dag.dag_id,
                "datei": dag.file,
                "zeile": dag.lineno,
                "schedule": dag.schedule_expr,
                "takt_s": dag.period_s,
            }
            for dag in dags
        ],
        "takt_s": takt_s,
        "takt_quelle": takt_quelle,
        "dauern_quelle": dauern_quelle,
        "statistik": statistic,
        "stichprobe_laeufe_min": min(ns) if ns else None,
        "stichprobe_laeufe_median": float(statistics.median(ns)) if ns else None,
        "anwendbar": analysis.lam is not None,
        "lambda_s": analysis.lam,
        "critical_path_s": analysis.critical_path_s,
        "critical_path_tasks": list(analysis.critical_path_tasks),
        "reserve_prozent": None,
        "drift_s_pro_lauf": None,
        "laeufe_bis_1h_rueckstand": None,
        **_urteil(analysis.lam, takt_s),
        "kritischer_kreis": kreis,
        "monte_carlo": mc,
        "what_if": what_if,
        "plan": build_plan(rows=what_if, analysis=analysis, dags=dags, takt_s=takt_s),
        "warnungen": warnungen,
        "modellgrenzen": MODELLGRENZEN,
    }


# --- Text-Ausgabe ----------------------------------------------------------------------


def _header(lang: Lang, key: str) -> list[str]:
    titel = t(lang, key)
    return ["", titel, "-" * len(titel)]


def _kopf(d: dict[str, Any], lang: Lang) -> list[str]:
    titel = t(lang, "report_title")
    zeilen = [titel, "=" * len(titel), ""]
    for dag in d["dags"]:
        name = dag["dag_id"] if dag["dag_id"] is not None else t(lang, "kopf_dag_id_missing")
        schedule = (
            t(lang, "kopf_schedule_suffix", schedule=dag["schedule"]) if dag["schedule"] else ""
        )
        zeilen.append(
            t(
                lang,
                "kopf_dag",
                name=name,
                datei=dag["datei"],
                zeile=dag["zeile"],
                schedule=schedule,
            )
        )
    if d["takt_s"] is not None:
        zeilen.append(t(lang, "kopf_takt", dauer=dur(d["takt_s"], lang), quelle=d["takt_quelle"]))
    else:
        zeilen.append(t(lang, "kopf_takt_unbekannt"))
    zeilen.append(t(lang, "kopf_dauern", quelle=d["dauern_quelle"]))
    zeilen.append(t(lang, "kopf_statistik", satz=t(lang, "stat_" + d["statistik"])))
    if d["stichprobe_laeufe_min"] is not None:
        zeilen.append(
            t(
                lang,
                "kopf_stichprobe",
                min=d["stichprobe_laeufe_min"],
                median=fmt(d["stichprobe_laeufe_median"], lang),
            )
        )
    return zeilen


def _urteil_text(d: dict[str, Any], lang: Lang) -> list[str]:
    zeilen = _header(lang, "urteil_header")
    lam, takt = d["lambda_s"], d["takt_s"]
    if d["urteil"] == "nicht_anwendbar":
        zeilen.append(t(lang, "urteil_nicht_anwendbar"))
        return zeilen
    if d["urteil"] == "takt_unbekannt":
        zeilen.append(t(lang, "urteil_takt_unbekannt", lam=dur(lam, lang)))
        return zeilen
    if d["urteil"] == "an_der_grenze":
        zeilen.append(t(lang, "urteil_an_der_grenze", lam=dur(lam, lang), takt=dur(takt, lang)))
        return zeilen
    if d["urteil"] == "stabil":
        zeilen.append(
            t(
                lang,
                "urteil_stabil",
                lam=dur(lam, lang),
                takt=dur(takt, lang),
                reserve=fmt(d["reserve_prozent"], lang),
            )
        )
        return zeilen
    drift = d["drift_s_pro_lauf"]
    laeufe = d["laeufe_bis_1h_rueckstand"]
    zeilen.append(
        t(
            lang,
            "urteil_instabil",
            lam=dur(lam, lang),
            takt=dur(takt, lang),
            drift=dur(drift, lang),
            laeufe=fmt(laeufe, lang),
            wanduhr=dur(laeufe * lam, lang),
        )
    )
    return zeilen


def _kreis_text(d: dict[str, Any], lang: Lang) -> list[str]:
    kreis = d["kritischer_kreis"]
    if kreis is None:
        return []
    zeilen = _header(lang, "kreis_header")
    zeilen.append(t(lang, "kreis_kondensiert_intro"))
    for kante in kreis["kondensiert"]:
        beleg = ""
        if kante["datei"] is not None:
            beleg = f" [{kante['signal']}, {kante['datei']}:{kante['zeile']}]"
        zeilen.append(
            t(
                lang,
                "kreis_kante",
                src=kante["src"],
                dst=kante["dst"],
                dauer=dur(kante["gewicht_s"], lang),
                perioden=perioden(kante["perioden"], lang),
                beleg=beleg,
            )
        )
        if len(kante["task_pfad"]) > 1:
            zeilen.append(t(lang, "kreis_task_pfad", pfad=" -> ".join(kante["task_pfad"])))
    zeilen.append(t(lang, "kreis_aufgeloest", pfad=" -> ".join(kreis["aufgeloest"])))
    zeilen.append(t(lang, "kreis_hinweis"))
    return zeilen


def _mc_text(d: dict[str, Any], lang: Lang) -> list[str]:
    zeilen = _header(lang, "mc_header")
    mc = d["monte_carlo"]
    if mc is None:
        zeilen.append(t(lang, "mc_aus"))
        return zeilen
    zeilen.append(
        t(
            lang,
            "mc_werte",
            p50=dur(mc["lambda_p50_s"], lang),
            p95=dur(mc["lambda_p95_s"], lang),
            samples=mc["samples"],
            seed=mc["seed"],
        )
    )
    zeilen.append(t(lang, "mc_p95_satz"))
    takt = d["takt_s"]
    if takt is not None:
        if mc["lambda_p50_s"] < takt < mc["lambda_p95_s"]:
            zeilen.append(t(lang, "mc_pendel"))
        if mc["anteil_ueber_takt"] is not None:
            zeilen.append(t(lang, "mc_anteil", p=fmt(mc["anteil_ueber_takt"] * 100, lang)))
    if mc["konstant_gesampelt"]:
        zeilen.append(t(lang, "mc_konstant", tasks=", ".join(mc["konstant_gesampelt"])))
    return zeilen


def _sammelzeile(kompakt: list[dict[str, Any]], lang: Lang) -> str:
    kreis = sum(1 for r in kompakt if r["auf_kreis"])
    extern = len(kompakt) - kreis
    teile = []
    if kreis:
        teile.append(
            t(lang, "sammel_kreis_1") if kreis == 1 else t(lang, "sammel_kreis_n", n=kreis)
        )
    if extern:
        teile.append(
            t(lang, "sammel_extern_1") if extern == 1 else t(lang, "sammel_extern_n", n=extern)
        )
    n = len(kompakt)
    kopf = t(lang, "sammel_kopf_1") if n == 1 else t(lang, "sammel_kopf_n", n=n)
    return t(lang, "sammel_zeile", kopf=kopf, teile=", ".join(teile))


def _plan_wirkung(zeile: dict[str, Any], lang: Lang) -> str:
    """Die Wirkungs-Klausel einer Aktionszeile: neues Lambda mit Delta absolut und
    Prozent, oder der Kreis ist ganz weg."""
    if zeile["lambda_neu_s"] is None:
        return t(lang, "plan_wirkung_kein_kreis")
    wirkung = t(lang, "plan_wirkung", dauer=dur(zeile["lambda_neu_s"], lang))
    if zeile.get("delta_s") is not None:
        p = zeile.get("delta_prozent")
        wirkung += t(
            lang,
            "plan_delta",
            vz="-" if zeile["delta_s"] < 0 else "+",
            n=fmt(abs(zeile["delta_s"]), lang),
            vzp="-" if (p is not None and p < 0) else "+",
            p=fmt(abs(p), lang) if p is not None else "0",
        )
    return wirkung


def _plan_gewinn_zeilen(zeile: dict[str, Any], plan: dict[str, Any], lang: Lang) -> list[str]:
    urteil, takt = plan["urteil"], plan["takt_s"]
    g = zeile["gewinn"]
    zeilen: list[str] = []
    if urteil == "instabil" and takt is not None:
        if zeile["macht_tragfaehig"] and "weggeraeumte_drift_s" in g:
            zeilen.append(
                t(lang, "plan_gewinn_tragfaehig", drift=dur(g["weggeraeumte_drift_s"], lang))
            )
        elif zeile["lambda_neu_s"] is not None:
            zeilen.append(t(lang, "plan_gewinn_nicht_tragfaehig", takt=dur(takt, lang)))
    elif urteil == "stabil" and "frische_delta_s" in g:
        zeilen.append(
            t(
                lang,
                "plan_gewinn_headroom",
                lam=dur(zeile["lambda_neu_s"], lang),
                takt=dur(takt, lang),
                mehr=fmt(g["laeufe_pro_tag_mehr"], lang),
                frische=dur(g["frische_delta_s"], lang),
            )
        )
    if zeile.get("katalog_schluessel"):
        zeilen.append(t(lang, "plan_fix_zeile", text=t(lang, zeile["katalog_schluessel"])))
    return zeilen


def _plan_text(d: dict[str, Any], lang: Lang) -> list[str]:
    """Der Beschleunigungsplan (Spec 012, ADR-024): jede Aktion als unbeanspruchte
    Reserve, mit Gewinn-Zeile und Behebungs-Muster. Ersetzt die What-if-Sektion."""
    plan = d["plan"]
    if plan["basis_lambda_s"] is None:
        return []
    zeilen = _header(lang, "plan_header")
    zeilen.append(t(lang, "plan_basis", dauer=dur(plan["basis_lambda_s"], lang)))
    hr = plan["headroom"]
    if hr is not None:
        zeilen.append(
            t(
                lang,
                "plan_headroom_intro",
                lam=dur(plan["basis_lambda_s"], lang),
                takt=dur(plan["takt_s"], lang),
                mehr=fmt(hr["laeufe_pro_tag_mehr"], lang),
                frische=dur(hr["frische_delta_s"], lang),
            )
        )
    for i, zeile in enumerate(plan["aktionen"], start=1):
        zeilen.append(
            t(
                lang,
                "plan_zeile",
                i=i,
                szenario=scenario_label(lang, zeile),
                wirkung=_plan_wirkung(zeile, lang),
            )
        )
        zeilen.extend(_plan_gewinn_zeilen(zeile, plan, lang))
    if plan["null_delta"]:
        zeilen.append(_sammelzeile(plan["null_delta"], lang))
    paar = plan["paar_rechnung"]
    if plan["kein_einzel_ausreichend"] and paar is not None:
        zeilen.append(t(lang, "plan_paar_intro", takt=dur(plan["takt_s"], lang)))
        zeilen.append(
            t(
                lang,
                "plan_paar_zeile",
                a=scenario_label(lang, paar["a"]),
                b=scenario_label(lang, paar["b"]),
                wirkung=_plan_wirkung(paar, lang),
            )
        )
    if hr is not None:
        zeilen.append(t(lang, "plan_headroom_fuss"))
    zeilen.append(t(lang, "plan_schluss"))
    return zeilen


def _warn_text(d: dict[str, Any], lang: Lang) -> list[str]:
    zeilen = _header(lang, "warn_header")
    if not d["warnungen"]:
        zeilen.append(t(lang, "warn_keine"))
    sensor_kreis = False
    f_befund = False
    for w in d["warnungen"]:
        art = w["art"]
        titel = t(lang, "warn_" + art) if ("warn_" + art) in CATALOG[lang] else art
        wo = w["task"] if w["task"] else ""
        if w["datei"] is not None:
            wo = f"{w['datei']}:{w['zeile']}"
        detail = f" ({w['detail']})" if w["detail"] else ""
        zeilen.append(t(lang, "warn_zeile", titel=titel, wo=wo, detail=detail))
        sensor_kreis = sensor_kreis or art == "sensor_im_kritischen_kreis"
        f_befund = f_befund or art == "prev_run_success"
    if sensor_kreis:
        zeilen.append("  " + t(lang, "sensor_kreis_text"))
    if f_befund:
        zeilen.append("  " + t(lang, "f_divergenz_text"))
    return zeilen


def render(d: dict[str, Any], lang: Lang = "en") -> str:
    grenzen = [t(lang, key) for key in ("modellgrenze_1", "modellgrenze_2", "modellgrenze_3")]
    zeilen = (
        _kopf(d, lang)
        + _urteil_text(d, lang)
        + _kreis_text(d, lang)
        + _plan_text(d, lang)
        + _mc_text(d, lang)
        + _warn_text(d, lang)
        + _header(lang, "modellgrenzen_header")
        + [t(lang, "modellgrenze_zeile", text=grenze) for grenze in grenzen]
    )
    zeilen.append("")
    return "\n".join(zeilen)
