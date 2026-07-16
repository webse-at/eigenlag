# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 012 — Beschleunigungsplan: aus der Diagnose wird das Produkt (2026-07-16)

**Der Report handelt jetzt.** Bis 011 sagte er „hier ist deine Grenze und wer schuld ist".
Ab 012 sagt der **Beschleunigungsplan**: „deine Pipeline könnte alle X laufen statt alle Y;
hier ist die Änderung, die den Unterschied kauft, und was sie bringt." Jeder Befund ist
unbeanspruchte Reserve, nicht Mangel. Details in `wiki/decisions.md` **ADR-024**.

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 012; Artefakte in `scan/012_plan/`)

| Was | Ergebnis |
|---|---|
| Plan-Modul | `eigenlag/plan.py::build_plan` reichert die What-if-Zeilen an: Kanten-Art (A–G, dbt-E), Katalog-Schlüssel, λ_neu, Delta absolut und Prozent, verdict-abhängige Gewinn-Felder. Reine Funktion, sprachneutral |
| Zwei Gewinn-Formen | **Instabil**: „makes your current schedule sustainable" gilt genau bei λ_neu < T, beziffert die weggeräumte Drift (λ − T); rettet keine Einzel-Aktion, folgt die Paar-Rechnung der drei wirksamsten. **Stabil**: Headroom — Läufe/Tag mehr (86400/λ − 86400/T) und „bis zu" (T − λ) frischer, plus Fußzeile „Untergrenze ohne Betriebsreserve" |
| Behebungs-Katalog | `messages.py` `plan_fix_*` je Kanten-Art in EN und DE, „üblicher Weg"/„commonly resolved by", nie Garantie. Vollständigkeit per Test erzwungen (jede Kanten-Art × beide Sprachen) |
| Report | Reihenfolge Urteil → Kreis → **Beschleunigungsplan** (ersetzt What-if) → Monte Carlo → Warnungen. `report._plan_text` rendert d["plan"] |
| `--json` | `plan`-Key **additiv**, `what_if` bleibt eingefroren daneben (Gate liest ihn), EN/DE byte-identisch (`diff -q` belegt) |

### Verifiziert (Belege gepastet in `wiki/log.md`, Artefakte in `scan/012_plan/`)

- `pytest`: **370 passed** (neu: `plan_test.py`, 16 Pins). Nur die 2 `durations`-Tests
  brauchen `sqlalchemy` (im Basis-Python nicht installiert, im `.venv` grün). `ruff check`,
  `ruff format --check`, `mypy eigenlag/` (27 Files) grün.
- **Demo** (Prototyp, λ = 4.40 h, T = 3.0 h, instabil): voller EN-Report,
  `scan/012_plan/lauf1_demo_plan_en.txt`. Das Marketing-Artefakt: die kostenlose
  Architektur-Änderung (`monitor → core` entfernt → 2.50 h) macht den Takt tragfähig
  (−43,18 %, räumt 84 min/Lauf Drift weg), das GPU-Upgrade (`retrain` halbiert → 3.60 h) nicht.
- **Flaggschiff** `load_data_wikiviews` (`--assume 300`, stabil, λ = 600 s, T = 3600 s):
  EN + DE (`lauf2_wikiviews_en.txt`/`_de.txt`), Headroom 120 Läufe/Tag mehr, bis zu 50 min frischer.
- **Synthetischer Zwei-Loop-Fall** (`lauf3_pair_en.txt`): keine Einzel-Aktion rettet T,
  Paar-Rechnung sichtbar (beide Selbst-Kanten zusammen → kein Kreis).
- `pipx install --force .`, Läufe über den Entry-Point; `--json` EN vs. DE byte-identisch.
- README-Quickstart zeigt einen echten, aktualisierten Lauf mit dem Plan-Abschnitt
  (`lauf4_readme_feature_pipeline.txt`, Fixture in `scan/012_plan/readme_demo/`).

## Hinweise für nächste Session

- **Für den Orchestrator zu prüfen (012):** der Header-Rename „What-if" → „Acceleration
  plan"/„Beschleunigungsplan". Die sammelzeilen-rendernden Report-Tests (009a) lasen früher
  `d["what_if"]`; sie zeigen jetzt auf `d["plan"]` (`plan_mit`/`paktion`-Helfer). Die
  JSON-`what_if`-Struktur-Tests blieben unverändert, der `plan`-Key ist rein additiv.
- **Bewusster Schnitt:** Die Kanten-Art eines `cross_entfernt` kommt aus der Signal-Herkunft
  der geparsten DAGs. Die Demo ist ein direkt gebautes Pipeline-Objekt ohne `ParsedDag` und
  trägt darum keinen Katalog-Text (kein erfundenes Detailwissen). Das ist gewollt.
- Der Katalog ist für D (`include_prior_dates`) und E (dbt `is_incremental`) vollständig,
  obwohl der Airflow-Parser für sie heute keine Kante erzeugt — Muster-Wissen, das greift,
  sobald die Kante existiert.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten. **dbt-Parser** bleibt bis nach dem Feedback-Meilenstein vertagt;
  erst dann erzeugt E echte Kanten und der Katalog-Eintrag `plan_fix_is_incremental` wird sichtbar.

## Was David entscheiden muss

1. Nichts Blockierendes im Code. Die eigentliche Entscheidung bleibt: wann das Repo public
   geht und in welcher Reihenfolge Reddit-Post / Airflow-Slack / Wikimedia-Kontakt. Die
   Launch-Texte sind Session 013 (Solo-Founder-Voicing, `wiki/roadmap.md`); dort wird die
   Euro-Übersetzung in Prosa gemacht, nicht im Tool (ADR-024, Punkt 3).
