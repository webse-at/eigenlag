"""Nachrichten-Kataloge fuer den Report und den Gate-Kommentar (Spec 011, ADR-023).

Englisch ist Default, Deutsch bleibt vollwertig unter --lang de. Zwei schlichte
dicts, keine i18n-Bibliothek. Ein Test (messages_test.py) erzwingt, dass beide
Kataloge exakt dieselben Keys tragen: ein fehlender Key wirft beim Rendern, statt
still auf die andere Sprache zu fallen.

compose()/compose_check() bleiben sprachneutral (--json ist ueber beide Sprachen
byte-identisch, die Werte darin sind deutsch eingefroren). Erst render()/
render_check() waehlen die Sprache. Deutsche Zahlen tragen ein Komma, englische
einen Punkt; die Einheiten (h, min, s) sind in beiden gleich.

Fachbegriffe bleiben unuebersetzt: Lambda/λ, Cross-Run, DAG, Task, Makespan.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

Lang = Literal["en", "de"]


def fmt(x: float, lang: Lang) -> str:
    """Zahl mit bis zu zwei Nachkommastellen, Nullen und Punkt abgeschnitten.
    Deutsch mit Komma, Englisch mit Punkt als Dezimaltrenner."""
    text = f"{x:.2f}".rstrip("0").rstrip(".")
    return text.replace(".", ",") if lang == "de" else text


def dur(seconds: float, lang: Lang) -> str:
    base = f"{fmt(seconds, lang)} s"
    if seconds >= 5400:
        return f"{base} ({fmt(seconds / 3600, lang)} h)"
    if seconds >= 120:
        return f"{base} ({fmt(seconds / 60, lang)} min)"
    return base


def perioden(n: int, lang: Lang) -> str:
    return t(lang, "perioden_1") if n == 1 else t(lang, "perioden_n", n=n)


def scenario_label(lang: Lang, row: Mapping[str, Any]) -> str:
    """Das What-if-Szenario-Label in der gewaehlten Sprache, aus den strukturierten
    Feldern der Zeile (art/task/wert_s/src/dst, angefragt). compose() fuellt dieselben
    Felder und baut daraus das deutsche 'szenario' fuer --json (ueber diese Funktion),
    render() baut daraus den sichtbaren Text pro Sprache. Eine Quelle fuer beide."""
    art = row["art"]
    angefragt = bool(row["angefragt"])
    if art == "task_halbiert":
        return t(lang, "whatif_task_halved", task=row["task"], sek=fmt(float(row["wert_s"]), lang))
    if art == "task_gesetzt":
        return t(lang, "whatif_task_set", task=row["task"], sek=fmt(float(row["wert_s"]), lang))
    base = t(lang, "whatif_edge_removed", src=row["src"], dst=row["dst"])
    return base + t(lang, "whatif_requested_suffix") if angefragt else base


DE: dict[str, str] = {
    # --- Report: Kopf ---
    "report_title": "eigenlag analyze",
    "kopf_dag": "DAG:        {name} ({datei}:{zeile}{schedule})",
    "kopf_dag_id_missing": "(dag_id nicht statisch)",
    "kopf_schedule_suffix": ", Schedule {schedule}",
    "kopf_takt": "Takt T:     {dauer}, Quelle: {quelle}",
    "kopf_takt_unbekannt": "Takt T:     unbekannt (kein statischer Schedule; --period setzt ihn)",
    "kopf_dauern": "Dauern:     {quelle}",
    "kopf_statistik": "Statistik:  {satz}",
    "kopf_stichprobe": "Stichprobe: Laeufe je Task minimal {min}, im Median {median}.",
    "stat_mean": (
        "mean. Fuer den asymptotischen Drift ist der Mittelwert die theoretisch richtige"
        " Groesse; er ist ausreisserempfindlich, ein einzelner haengender Lauf kann ihn"
        " deutlich verschieben."
    ),
    "stat_p50": (
        "p50. Der Median ist robust gegen Ausreisser, unterschaetzt aber den Drift,"
        " wenn die Dauern rechtsschief streuen."
    ),
    "stat_p95": "p95. Bewusst pessimistisch: Lambda einer durchgehend schlechten Woche.",
    # --- Report: Urteil ---
    "urteil_header": "Urteil",
    "urteil_nicht_anwendbar": (
        "Nicht anwendbar: keine Cross-Run-Kante. Kein Lauf dieses DAGs wartet auf"
        " einen frueheren Lauf, es gibt keinen Kreis ueber die Zeitachse und damit"
        " keine strukturelle Taktgrenze. Der Takt wird allein von Kapazitaet und"
        " Laufzeit begrenzt, nicht von der Abhaengigkeitsstruktur."
    ),
    "urteil_takt_unbekannt": (
        "Lambda = {lam}: schneller kann diese Pipeline dauerhaft nicht"
        " takten. Der Takt T ist nicht bekannt (Schedule nicht statisch aufloesbar"
        " oder dataset-getriggert), deshalb gibt es kein Urteil stabil oder"
        " instabil. Mit --period SEKUNDEN wird der Vergleich gerechnet."
    ),
    "urteil_an_der_grenze": (
        "An der Grenze: Lambda = {lam} liegt innerhalb von 10 Prozent am"
        " Takt T = {takt}. Vorsicht bei der Deutung: Systeme, deren Tasks"
        " auf Daten der laufenden Periode warten, pendeln sich genau hier ein, und"
        " die gemessenen Dauern sind dann bereits das Ergebnis dieses"
        " eingeschwungenen Zustands. Ob die Pipeline stabil ist oder driftet,"
        " entscheidet an dieser Grenze die Rueckkopplung, nicht die Messung."
    ),
    "urteil_stabil": (
        "Stabil: Lambda = {lam} liegt unter dem Takt T = {takt}."
        " Reserve: {reserve} %. Verspaetungen aus einem"
        " einzelnen Lauf klingen ab, statt sich aufzubauen."
    ),
    "urteil_instabil": (
        "Instabil: Lambda = {lam} liegt ueber dem Takt T = {takt}."
        " Die Verspaetung waechst um {drift} pro Lauf, unbegrenzt und"
        " unabhaengig von der Worker-Anzahl. Eine Stunde Rueckstand ist nach"
        " {laeufe} Laeufen erreicht (etwa {wanduhr} Wanduhr-Zeit)."
        " Mehr Rechenleistung aendert daran nichts, weil der Engpass die"
        " Abhaengigkeitsstruktur ist, nicht die Kapazitaet."
    ),
    # --- Report: Kritischer Kreis ---
    "kreis_header": "Kritischer Kreis",
    "kreis_kondensiert_intro": (
        "Kondensiert (der Kreis in der Cross-Run-Matrix, sein Zyklusmittel ist Lambda):"
    ),
    "kreis_kante": "  {src} -> {dst}: Gewicht {dauer}, {perioden}{beleg}",
    "kreis_task_pfad": "    als Task-Pfad: {pfad}",
    "kreis_aufgeloest": "Aufgeloest ueber alle Segmente: {pfad}",
    "kreis_hinweis": (
        "Der Weg zu einem kleineren Lambda fuehrt ueber diesen Kreis; eine Verkuerzung"
        " daneben aendert Lambda um exakt null. Ob eine einzelne Verkuerzung"
        " durchschlaegt oder ein zweiter Kreis mit gleichem Zyklusmittel uebernimmt,"
        " rechnet der Beschleunigungsplan unten nach."
    ),
    "perioden_1": "1 Periode zurueck",
    "perioden_n": "{n} Perioden zurueck",
    # --- Report: Monte Carlo ---
    "mc_header": "Monte Carlo",
    "mc_aus": (
        "Nicht gerechnet (abgeschaltet oder kein Kreis). Die Lambda-Angabe oben ist"
        " ein Punktwert auf der gewaehlten Statistik."
    ),
    "mc_werte": (
        "Lambda p50 = {p50}, Lambda p95 = {p95}"
        " ({samples} Stichproben, Lognormal-Fit aus p50/p95 je Task, Seed"
        " {seed}: derselbe Aufruf liefert dieselben Zahlen)."
    ),
    "mc_p95_satz": (
        "p95 beantwortet, ob der Takt auch in einer schlechten Woche haelt, nicht nur"
        " im Durchschnitt."
    ),
    "mc_pendel": (
        "Instabil in schlechten Wochen, erholt sich in guten: die Verspaetung"
        " pendelt statt zu wachsen. Sichtbar wird das als Pipeline, die"
        " gelegentlich hinterherlaeuft und sich scheinbar grundlos wieder faengt."
    ),
    "mc_anteil": "Anteil der Stichproben mit Lambda ueber dem Takt: {p} %.",
    "mc_konstant": (
        "Konstant gesampelt (keine belastbare Streuung, angenommene oder duenne"
        " Dauern): {tasks}. Die p95-Aussage unterschaetzt die Streuung dieser Tasks."
    ),
    # --- What-if-Szenario-Labels (scenario_label, geteilt von Plan und Gate) ---
    "whatif_task_halved": "Task {task} halbiert (auf {sek} s)",
    "whatif_task_set": "Task {task} = {sek} s (angefragt)",
    "whatif_edge_removed": "Cross-Kante {src} -> {dst} entfernt",
    "whatif_requested_suffix": " (angefragt)",
    # --- Report: Beschleunigungsplan (Spec 012, ADR-024) ---
    "plan_header": "Beschleunigungsplan",
    "plan_basis": (
        "Basis: Lambda = {dauer}. Jede Aktion ist unbeanspruchte Reserve, sortiert nach"
        " neuem Lambda."
    ),
    "plan_zeile": "  {i}. {szenario}: {wirkung}",
    "plan_wirkung": "Lambda {dauer}",
    "plan_wirkung_kein_kreis": "kein Kreis mehr, Taktgrenze aufgeloest",
    "plan_delta": ", {vz}{n} s ({vzp}{p} %)",
    "plan_fix_zeile": "     ueblicher Weg: {text}",
    "plan_gewinn_tragfaehig": (
        "     macht den laufenden Takt tragfaehig und raeumt die Drift von {drift} pro Lauf weg."
    ),
    "plan_gewinn_nicht_tragfaehig": (
        "     senkt Lambda, macht den Takt T = {takt} fuer sich allein aber nicht tragfaehig."
    ),
    "plan_gewinn_headroom": (
        "     Untergrenze faellt von Lambda auf {lam}; ein Takt von {lam} statt T = {takt}"
        " liefe {mehr} mal pro Tag oefter und hielte die Daten jederzeit bis zu {frische} frischer."
    ),
    "plan_headroom_intro": (
        "Lambda = {lam} ist die nachhaltige Untergrenze. Ein Takt von {lam} statt T = {takt}"
        " liefe {mehr} mal pro Tag oefter und hielte die Daten jederzeit bis zu {frische} frischer."
    ),
    "plan_headroom_fuss": (
        "Lambda ist eine Untergrenze ohne Betriebsreserve; der Plan beziffert die Reserve,"
        " er empfiehlt keinen konkreten neuen Takt."
    ),
    "plan_paar_intro": (
        "Keine einzelne Standard-Aenderung macht T = {takt} tragfaehig. Die zwei"
        " guenstigsten zusammen:"
    ),
    "plan_paar_zeile": "  {a} + {b}: {wirkung}.",
    "sammel_kopf_1": "1 weiteres Szenario aendert Lambda nicht",
    "sammel_kopf_n": "{n} weitere Szenarien aendern Lambda nicht",
    "sammel_kreis_1": "1 Kreis-Gleichstand",
    "sammel_kreis_n": "{n} Kreis-Gleichstaende",
    "sammel_extern_1": "1 Kante ausserhalb des kritischen Kreises",
    "sammel_extern_n": "{n} Kanten ausserhalb des kritischen Kreises",
    "sammel_zeile": "  {kopf}: {teile}.",
    "plan_schluss": (
        "Eine Aenderung, die nicht auf dem kritischen Kreis liegt, aendert Lambda um"
        " exakt null. Der Plan rechnet deshalb die Kreis-Tasks und alle Cross-Kanten"
        " durch; was Lambda nicht aendert, ist fuer die Taktgrenze wirkungslos, so"
        " nuetzlich es fuer die Latenz eines Einzellaufs sein mag."
    ),
    # --- Behebungs-Katalog: Muster-Wissen je Kanten-Art (ADR-024), nie eine Garantie ---
    "plan_fix_depends_on_past": (
        "pruefen, ob der Task den Vorlauf-Output braucht oder nur dessen Reihenfolge;"
        " idempotente, partitionierte Inkremente brauchen ihn oft nicht."
    ),
    "plan_fix_wait_for_downstream": (
        "schuetzt meist vor ueberlappenden Writes; mit Partitions-Isolation (jeder Lauf"
        " schreibt seine eigene Partition) ist Ueberlappung sicher."
    ),
    "plan_fix_external_task_sensor": (
        "Polling durch einen Dataset-/Asset-Trigger ersetzen oder pruefen, ob der Versatz"
        " kleiner sein kann."
    ),
    "plan_fix_include_prior_dates": (
        "meist eine Vorsichts-Einstellung; pruefen, ob der konkrete Vorlauf wirklich"
        " gebraucht wird."
    ),
    "plan_fix_max_active_runs": (
        "oft eine Pauschal-Sicherung; schreiben die Laeufe partitions-isoliert, ist"
        " Ueberlappung sicher."
    ),
    "plan_fix_is_incremental": (
        "pruefen, ob das Model wirklich aus seiner eigenen Zieltabelle lesen muss oder das"
        " Inkrement aus der Quelle rekonstruierbar ist."
    ),
    "plan_fix_task_halved": (
        "Task teilen, Inkrement verkleinern oder Warm-Start; der Plan nennt hier die"
        " Rechnung, kein Detailwissen ueber den fremden Task."
    ),
    # --- Report: Warnungen ---
    "warn_header": "Warnungen",
    "warn_keine": "Keine.",
    "warn_zeile": "  - {titel}: {wo}{detail}",
    "warn_sensor_im_kritischen_kreis": "Sensor auf dem kritischen Kreis",
    "warn_dauer_angenommen": "Dauer angenommen",
    "warn_stichprobe_zu_klein": "Stichprobe zu klein",
    "warn_sensor_not_modeled": "Sensor-Kante nicht modelliert",
    "warn_sensor_dynamic_offset": "Sensor-Versatz nicht statisch bestimmbar",
    "warn_include_prior_dates": "include_prior_dates nicht modelliert",
    "warn_prev_run_success": "prev_*_success-Zugriff (keine Lambda-Kante)",
    "warn_prev_run_date": "prev_ds-Zugriff (schwaches Signal)",
    "sensor_kreis_text": (
        "Die gemessene Dauer eines Sensors enthaelt Wartezeit auf externe Ereignisse und"
        " laesst sich aus der Metadaten-DB nicht von Arbeitszeit trennen. Lambda kann"
        " dadurch ueberschaetzt sein und ist keine harte Untergrenze mehr. Wartet der"
        " Sensor auf Daten der laufenden Periode, koppelt er die Pipeline an die Wanduhr:"
        " solche Systeme pendeln sich genau an ihrer Taktgrenze ein, und die gemessenen"
        " Dauern sind bereits das Ergebnis dieses eingeschwungenen Zustands."
    ),
    "f_divergenz_text": (
        "Hinweis zu prev_*_success: der Zugriff zaehlt als Cross-Run-Befund, erzeugt aber"
        " keine Lambda-Kante. Das Template rendert einen Zeitstempel und wartet nicht;"
        " ein Task damit startet puenktlich und liest schlimmstenfalls veraltete Daten."
        " Das ist ein Korrektheits-, kein Durchsatz-Problem."
    ),
    # --- Report: Modellgrenzen ---
    "modellgrenzen_header": "Modellgrenzen",
    "modellgrenze_zeile": "  - {text}",
    "modellgrenze_1": (
        "Unbegrenzte Parallelitaet angenommen: Lambda ist eine Untergrenze der realen"
        " Taktzeit. Das Tool sagt 'nicht schneller als Lambda', nicht 'Lambda ist erreichbar'."
    ),
    "modellgrenze_2": (
        "Retries, Sensor-Poking und Pool-Limits sind nicht modelliert. Sie koennen die"
        " reale Taktzeit nur erhoehen, nie senken; die Untergrenze bleibt gueltig."
    ),
    "modellgrenze_3": (
        "Latenz-Angaben sind Makespan: die Dauer eines Laufs von seinem Start bis zum"
        " Ende seines laengsten Pfads, nicht die Verspaetung gegenueber dem Plan."
    ),
    # --- Gate: Kopf und Rahmen ---
    "check_bestanden": (
        "**eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt"
        " (`{pfad}` gegen `{ref}`)."
    ),
    "check_ausgeloest": "**eigenlag check: ausgeloest** — {grund}.",
    "check_struktur_hinweis": (
        "Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine"
        " Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder"
        " --assume-duration."
    ),
    "check_modellgrenzen_kurz": (
        "Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist"
        " angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie"
        " koennen die reale Taktzeit nur erhoehen, nie senken."
    ),
    "check_lam_kein_kreis": "kein Kreis",
    "check_lam_einheit_1": "1 Task-Einheit",
    "check_lam_einheit_n": "{n} Task-Einheiten",
    "check_grund_suffix": " (und {n} weitere DAGs)",
    # --- Gate: DAG-Abschnitt ---
    "check_abschnitt_lambda": "- Lambda: {vorher} -> {nachher} (vorher -> nachher)",
    "check_existierte_nicht": "existierte nicht",
    "check_geloescht": "geloescht",
    "check_abschnitt_takt": "- Takt T: {dauer}, Quelle: {quelle}",
    "check_abschnitt_takt_unbekannt": (
        "- Takt T: unbekannt (kein statischer Schedule; --period setzt ihn)"
    ),
    "check_abschnitt_neue_kanten": "- Neue Cross-Run-Kanten ({n}):",
    "check_kanten_zeile": "  - `{src} -> {dst}` ({signal}, {datei}:{zeile}, {perioden})",
    "check_abschnitt_entfallene": "- Entfallene Cross-Run-Kanten: {liste}",
    "check_abschnitt_ausgeloest": "- **Ausgeloest:** {grund}",
    "check_abschnitt_ausloeser": (
        "- **Ausloesende Kante:** `{src} -> {dst}` ({signal}, {datei}:{zeile})"
    ),
    "check_abschnitt_kreis": (
        "- Kritischer Kreis, kondensiert: `{src} -> {dst}`, Gewicht {gewicht}, {perioden}{beleg}"
    ),
    "check_abschnitt_aufgeloest": "- Aufgeloest: {pfad}",
    "check_abschnitt_behebung": "- Behebung: {text}",
    "check_unveraendert": "{n} DAG(s) ohne Aenderung an Cross-Run-Kanten oder Lambda.",
    "check_modellgrenzen_fuss": "_{text}_",
    # --- Gate: Fail-Gruende ---
    "grund_struktur_kreis": (
        "neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse"
        " bei sub-taeglichem Takt (T = {takt})"
    ),
    "grund_lambda_ueber_t": ("neue Cross-Run-Kante und Lambda = {lam} ueber dem Takt T = {takt}"),
    "grund_kante_1": "1 neue Cross-Run-Kante",
    "grund_kante_n": "{n} neue Cross-Run-Kanten",
    "grund_fail_on_new_edge": "{anzahl} (--fail-on-new-edge)",
    "grund_max_increase_neu": (
        "Lambda-Anstieg ueber {schranke} % (--max-increase):"
        " vorher kein Kreis, nachher eine Taktgrenze"
    ),
    "grund_max_increase": (
        "Lambda-Anstieg {anstieg} % ueber der Schranke {schranke} % (--max-increase)"
    ),
    # --- Gate: Behebung ---
    "behebung_struktur": (
        "Die ausloesende Kante zu entfernen behebt den Fail. Eine Zeit-Aussage"
        " (Lambda gegen T in Sekunden) braucht eine Dauern-Quelle: --db oder"
        " --assume-duration."
    ),
    "behebung_kein_takt": "Kein Takt bekannt; --period setzt ihn, sonst gibt es kein Unter-T-Ziel.",
    "behebung_bestes": "{szenario} bringt Lambda auf {lam} und damit unter T = {takt}.",
    "behebung_aufloesung": (
        "Keine einzelne Standard-Aenderung bringt Lambda unter T, aber"
        " {szenario} loest den Kreis ganz auf (keine Taktgrenze mehr)."
    ),
    "behebung_keine": (
        "Keine einzelne Standard-Aenderung (Kreis-Task halbiert, Cross-Kante entfernt)"
        " bringt Lambda unter T; der Kreis traegt an mehreren Stellen dasselbe Zyklusmittel."
    ),
    # --- Gate: Hinweise ---
    "hinweis_unbenannt": (
        "{n} DAG-Definition(en) ohne statisch aufloesbare dag_id"
        " sind nicht vergleichbar und bleiben aussen vor: {stellen}"
    ),
    "hinweis_keine_dags": "Keine DAGs in beiden Staenden — nichts zu pruefen.",
}


EN: dict[str, str] = {
    # --- Report: header ---
    "report_title": "eigenlag analyze",
    "kopf_dag": "DAG:        {name} ({datei}:{zeile}{schedule})",
    "kopf_dag_id_missing": "(dag_id not static)",
    "kopf_schedule_suffix": ", schedule {schedule}",
    "kopf_takt": "Period T:   {dauer}, source: {quelle}",
    "kopf_takt_unbekannt": "Period T:   unknown (no static schedule; set it with --period)",
    "kopf_dauern": "Durations:  {quelle}",
    "kopf_statistik": "Statistic:  {satz}",
    "kopf_stichprobe": "Sample:     runs per task, minimum {min}, median {median}.",
    "stat_mean": (
        "mean. For the asymptotic drift the mean is the theoretically correct quantity;"
        " it is sensitive to outliers, and a single hanging run can shift it noticeably."
    ),
    "stat_p50": (
        "p50. The median is robust against outliers but understates the drift when the"
        " durations are right-skewed."
    ),
    "stat_p95": "p95. Deliberately pessimistic: λ of a consistently bad week.",
    # --- Report: verdict ---
    "urteil_header": "Verdict",
    "urteil_nicht_anwendbar": (
        "Not applicable: no cross-run edge. No run of this DAG waits on an earlier run,"
        " so there is no cycle across the time axis and no structural cycle limit. The"
        " achievable period is bounded only by capacity and runtime, not by the"
        " dependency structure."
    ),
    "urteil_takt_unbekannt": (
        "λ = {lam}: this pipeline cannot sustainably run any faster. The period T is"
        " unknown (the schedule is not statically resolvable or is dataset-triggered),"
        " so there is no stable-or-unstable verdict. Pass --period SECONDS to compute"
        " the comparison."
    ),
    "urteil_an_der_grenze": (
        "At the limit: λ = {lam} lies within 10 percent of the period T = {takt}. Read"
        " this with care: systems whose tasks wait on data from the current period"
        " settle exactly here, and the measured durations are then already the result of"
        " that steady state. Whether the pipeline is stable or drifts is decided at this"
        " limit by the feedback, not by the measurement."
    ),
    "urteil_stabil": (
        "Stable: λ = {lam} lies below the period T = {takt}. Headroom: {reserve} %."
        " Delays from a single run fade out instead of building up."
    ),
    "urteil_instabil": (
        "Unstable: λ = {lam} lies above the period T = {takt}. The delay grows by"
        " {drift} per run, without bound and regardless of the number of workers. One"
        " hour of backlog is reached after {laeufe} runs (about {wanduhr} of wall-clock"
        " time). More compute changes nothing, because the bottleneck is the dependency"
        " structure, not the capacity."
    ),
    # --- Report: critical cycle ---
    "kreis_header": "Critical cycle",
    "kreis_kondensiert_intro": (
        "Condensed (the cycle in the cross-run matrix; its cycle mean is λ):"
    ),
    "kreis_kante": "  {src} -> {dst}: weight {dauer}, {perioden}{beleg}",
    "kreis_task_pfad": "    as task path: {pfad}",
    "kreis_aufgeloest": "Resolved across all segments: {pfad}",
    "kreis_hinweis": (
        "The path to a smaller λ runs through this cycle; a shortening anywhere else"
        " changes λ by exactly zero. Whether a single shortening carries through or a"
        " second cycle with the same cycle mean takes over is what the acceleration plan"
        " below computes."
    ),
    "perioden_1": "1 period back",
    "perioden_n": "{n} periods back",
    # --- Report: Monte Carlo ---
    "mc_header": "Monte Carlo",
    "mc_aus": (
        "Not computed (disabled or no cycle). The λ above is a point value on the chosen statistic."
    ),
    "mc_werte": (
        "λ p50 = {p50}, λ p95 = {p95}"
        " ({samples} samples, lognormal fit from p50/p95 per task, seed"
        " {seed}: the same call yields the same numbers)."
    ),
    "mc_p95_satz": (
        "p95 answers whether the period also holds in a bad week, not just on average."
    ),
    "mc_pendel": (
        "Unstable in bad weeks, recovers in good ones: the delay swings instead of"
        " growing. It shows up as a pipeline that occasionally falls behind and"
        " seemingly catches up again for no reason."
    ),
    "mc_anteil": "Share of samples with λ above the period: {p} %.",
    "mc_konstant": (
        "Sampled as constant (no reliable spread, assumed or thin durations): {tasks}."
        " The p95 statement understates the spread of these tasks."
    ),
    # --- what-if scenario labels (scenario_label, shared by plan and gate) ---
    "whatif_task_halved": "task {task} halved (to {sek} s)",
    "whatif_task_set": "task {task} = {sek} s (requested)",
    "whatif_edge_removed": "cross-run edge {src} -> {dst} removed",
    "whatif_requested_suffix": " (requested)",
    # --- Report: acceleration plan (Spec 012, ADR-024) ---
    "plan_header": "Acceleration plan",
    "plan_basis": ("Base: λ = {dauer}. Each action is unclaimed reserve, sorted by the new λ."),
    "plan_zeile": "  {i}. {szenario}: {wirkung}",
    "plan_wirkung": "λ {dauer}",
    "plan_wirkung_kein_kreis": "no cycle left, cycle limit removed",
    "plan_delta": ", {vz}{n} s ({vzp}{p} %)",
    "plan_fix_zeile": "     commonly resolved by: {text}",
    "plan_gewinn_tragfaehig": (
        "     makes your current schedule sustainable and removes the {drift} of drift per run."
    ),
    "plan_gewinn_nicht_tragfaehig": (
        "     lowers λ but does not make the period T = {takt} sustainable on its own."
    ),
    "plan_gewinn_headroom": (
        "     floor drops from λ to {lam}; a schedule of {lam} instead of T = {takt} would"
        " run {mehr} more times per day and keep data up to {frische} fresher at any moment."
    ),
    "plan_headroom_intro": (
        "λ = {lam} is the sustainable floor. A schedule of {lam} instead of T = {takt} would"
        " run {mehr} more times per day and keep data up to {frische} fresher at any moment."
    ),
    "plan_headroom_fuss": (
        "λ is a floor without operating reserve; the plan quantifies the reserve, it does"
        " not recommend a concrete new schedule."
    ),
    "plan_paar_intro": (
        "No single standard change makes T = {takt} sustainable. The two cheapest in combination:"
    ),
    "plan_paar_zeile": "  {a} + {b}: {wirkung}.",
    "sammel_kopf_1": "1 more scenario leaves λ unchanged",
    "sammel_kopf_n": "{n} more scenarios leave λ unchanged",
    "sammel_kreis_1": "1 cycle tie",
    "sammel_kreis_n": "{n} cycle ties",
    "sammel_extern_1": "1 edge off the critical cycle",
    "sammel_extern_n": "{n} edges off the critical cycle",
    "sammel_zeile": "  {kopf}: {teile}.",
    "plan_schluss": (
        "A change that does not lie on the critical cycle changes λ by exactly zero. The"
        " plan therefore computes the cycle tasks and all cross-run edges; what does not"
        " change λ is irrelevant to the cycle limit, however useful it may be for the"
        " latency of a single run."
    ),
    # --- Fix catalog: pattern knowledge per edge type (ADR-024), never a guarantee ---
    "plan_fix_depends_on_past": (
        "check whether the task needs the predecessor's output or only its ordering;"
        " idempotent, partitioned increments often do not."
    ),
    "plan_fix_wait_for_downstream": (
        "usually guards against overlapping writes; with partition isolation (each run"
        " writes its own partition) overlap is safe."
    ),
    "plan_fix_external_task_sensor": (
        "replace polling with a dataset/asset trigger, or check whether the offset can be smaller."
    ),
    "plan_fix_include_prior_dates": (
        "usually a precaution; check whether the concrete prior run is really needed."
    ),
    "plan_fix_max_active_runs": (
        "often a blanket safeguard; if runs write partition-isolated, overlap is safe."
    ),
    "plan_fix_is_incremental": (
        "check whether the model must read from its own target table or the increment can"
        " be rebuilt from source."
    ),
    "plan_fix_task_halved": (
        "split the task, shrink the increment, or warm-start; the plan gives the arithmetic"
        " here, not detail about the foreign task."
    ),
    # --- Report: warnings ---
    "warn_header": "Warnings",
    "warn_keine": "None.",
    "warn_zeile": "  - {titel}: {wo}{detail}",
    "warn_sensor_im_kritischen_kreis": "Sensor on the critical cycle",
    "warn_dauer_angenommen": "Duration assumed",
    "warn_stichprobe_zu_klein": "Sample too small",
    "warn_sensor_not_modeled": "Sensor edge not modeled",
    "warn_sensor_dynamic_offset": "Sensor offset not statically determinable",
    "warn_include_prior_dates": "include_prior_dates not modeled",
    "warn_prev_run_success": "prev_*_success access (no λ edge)",
    "warn_prev_run_date": "prev_ds access (weak signal)",
    "sensor_kreis_text": (
        "The measured duration of a sensor includes waiting time for external events and"
        " cannot be separated from work time in the metadata DB. λ may therefore be"
        " overestimated and is no longer a hard lower bound. If the sensor waits on data"
        " from the current period, it couples the pipeline to the wall clock: such"
        " systems settle exactly at their cycle limit, and the measured durations are"
        " already the result of that steady state."
    ),
    "f_divergenz_text": (
        "Note on prev_*_success: the access counts as a cross-run finding but creates no"
        " λ edge. The template renders a timestamp and does not wait; a task using it"
        " starts on time and at worst reads stale data. That is a correctness problem,"
        " not a throughput one."
    ),
    # --- Report: model limits ---
    "modellgrenzen_header": "Model limits",
    "modellgrenze_zeile": "  - {text}",
    "modellgrenze_1": (
        "Unbounded parallelism assumed: λ is a lower bound of the real cycle time. The"
        " tool says 'no faster than λ', not 'λ is achievable'."
    ),
    "modellgrenze_2": (
        "Retries, sensor poking and pool limits are not modeled. They can only raise the"
        " real cycle time, never lower it; the lower bound stays valid."
    ),
    "modellgrenze_3": (
        "Latency figures are makespan: the duration of one run from its start to the end"
        " of its longest path, not the delay against the plan."
    ),
    # --- Gate: header and frame ---
    "check_bestanden": (
        "**eigenlag check: passed.** No change raises λ above the period (`{pfad}` vs `{ref}`)."
    ),
    "check_ausgeloest": "**eigenlag check: triggered** — {grund}.",
    "check_struktur_hinweis": (
        "Structural comparison: λ in task units (uniform duration 1.0 per task, no"
        " duration source given). For λ in seconds against the period: --db or"
        " --assume-duration."
    ),
    "check_modellgrenzen_kurz": (
        "λ is a lower bound of the real cycle time: unbounded parallelism is assumed."
        " Retries, sensor poking and pool limits are not modeled; they can only raise"
        " the real cycle time, never lower it."
    ),
    "check_lam_kein_kreis": "no cycle",
    "check_lam_einheit_1": "1 task unit",
    "check_lam_einheit_n": "{n} task units",
    "check_grund_suffix": " (and {n} more DAGs)",
    # --- Gate: DAG section ---
    "check_abschnitt_lambda": "- λ: {vorher} -> {nachher} (before -> after)",
    "check_existierte_nicht": "did not exist",
    "check_geloescht": "deleted",
    "check_abschnitt_takt": "- Period T: {dauer}, source: {quelle}",
    "check_abschnitt_takt_unbekannt": (
        "- Period T: unknown (no static schedule; set it with --period)"
    ),
    "check_abschnitt_neue_kanten": "- New cross-run edges ({n}):",
    "check_kanten_zeile": "  - `{src} -> {dst}` ({signal}, {datei}:{zeile}, {perioden})",
    "check_abschnitt_entfallene": "- Removed cross-run edges: {liste}",
    "check_abschnitt_ausgeloest": "- **Triggered:** {grund}",
    "check_abschnitt_ausloeser": (
        "- **Triggering edge:** `{src} -> {dst}` ({signal}, {datei}:{zeile})"
    ),
    "check_abschnitt_kreis": (
        "- Critical cycle, condensed: `{src} -> {dst}`, weight {gewicht}, {perioden}{beleg}"
    ),
    "check_abschnitt_aufgeloest": "- Resolved: {pfad}",
    "check_abschnitt_behebung": "- Fix: {text}",
    "check_unveraendert": "{n} DAG(s) with no change to cross-run edges or λ.",
    "check_modellgrenzen_fuss": "_{text}_",
    # --- Gate: failure reasons ---
    "grund_struktur_kreis": (
        "new cross-run edge closes a cycle across the time axis at a sub-daily period (T = {takt})"
    ),
    "grund_lambda_ueber_t": "new cross-run edge and λ = {lam} above the period T = {takt}",
    "grund_kante_1": "1 new cross-run edge",
    "grund_kante_n": "{n} new cross-run edges",
    "grund_fail_on_new_edge": "{anzahl} (--fail-on-new-edge)",
    "grund_max_increase_neu": (
        "λ increase above {schranke} % (--max-increase): no cycle before, a cycle limit after"
    ),
    "grund_max_increase": (
        "λ increase {anstieg} % over the {schranke} % threshold (--max-increase)"
    ),
    # --- Gate: fix hint ---
    "behebung_struktur": (
        "Removing the triggering edge fixes the failure. A time statement (λ against T"
        " in seconds) needs a duration source: --db or --assume-duration."
    ),
    "behebung_kein_takt": (
        "No period known; --period sets it, otherwise there is no under-T target."
    ),
    "behebung_bestes": "{szenario} brings λ to {lam} and thus below T = {takt}.",
    "behebung_aufloesung": (
        "No single standard change brings λ below T, but"
        " {szenario} dissolves the cycle entirely (no cycle limit left)."
    ),
    "behebung_keine": (
        "No single standard change (halving a cycle task, removing a cross-run edge)"
        " brings λ below T; the cycle carries the same cycle mean at several places."
    ),
    # --- Gate: notes ---
    "hinweis_unbenannt": (
        "{n} DAG definition(s) without a statically resolvable dag_id"
        " cannot be compared and are left out: {stellen}"
    ),
    "hinweis_keine_dags": "No DAGs in both states, nothing to check.",
}


CATALOG: dict[Lang, dict[str, str]] = {"en": EN, "de": DE}


def t(lang: Lang, key: str, **kw: object) -> str:
    template = CATALOG[lang][key]
    return template.format(**kw) if kw else template
