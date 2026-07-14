# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 007 — Airflow-Parser: vom DAG-File zur Pipeline (2026-07-14)

**Der Parser-Layer steht.** `eigenlag/parse_airflow.py` übersetzt DAG-Files per AST in
`ParsedDag` (Tasks, Intra-Kanten, Cross-Kanten mit Herkunft Datei:Zeile:Signal, Warnungen)
und `to_pipeline(dags, durations=1.0)` heiratet die Struktur mit Dauern. Leitregel
durchgezogen: nicht statisch Auflösbares wird Warnung, nie Kante — λ bleibt Untergrenze.
`scanner/schedule.py` ist nach `eigenlag/schedule.py` umgezogen (Abhängigkeit Produkt ← Scanner).

### Die Zahlen (alle aus `scan/007_parse/`, Lauf 288 s über den Clone-Cache)

| Größe | Wert |
|---|---|
| Kandidaten-Files geparst (Kern 176 + G-only 473 aus `scan/v2/`) | 626 von 626, **0 Syntax-Fehler** |
| DAGs gefunden / mit statischer `dag_id` | 4892 / 3583 (73,2 %) |
| Kandidaten-Zeilen wiedergefunden | 646 von 649; die 3 fehlenden = `dag_not_airflow` (belegt korrekt) |
| Konsistenz Parser ↔ Scanner | 3 Abweichungen, alle = dokumentierte Import-Beleg-Differenz |
| **Karp = Howard** (offener Punkt aus Abnahme 004) | **auf allen 4836 kondensierten Graphen**, 4827 zusätzlich per Brute-Force, 0 Abweichungen |
| Statisch modellierbare Sensor-Kanten (C) im Korpus | **0** — 34 Fälle, jeder mit konkretem Grund in `warnings.jsonl` |
| **Teilpfad-Fälle (λ < Critical Path, uniforme Dauern)** | **129 Kern-Kandidaten-DAGs in 77 Repos** (`teilpfad.csv`, je mit Permalink) |

**Der Produkt-Fall existiert in öffentlichem Code** (Auflage aus ADR-019 erfüllt).
Durchgerechnet im Log: `udac_example_dag` (Udacity-Sparkify), `wait_for_downstream` in
`default_args` → Kreis aus 2 Tasks, λ = 2,0 gegen Critical Path 6,0. Ein Dashboard sieht 6,
die Taktgrenze ist 2. Alles Struktur-Aussagen (Dauer 1.0 je Task), keine Zeit-Aussagen.

### Was sonst passiert ist

- **ADR-020**: F zählt für die Marktzahl, erzeugt aber keine λ-Kante (Template rendert,
  wartet nicht). Divergenz gehört in den Report-Text von Session 009.
- `wiki/signals.md` hat jetzt die λ-Übersetzungstabelle; `architecture.md` und `roadmap.md`
  nachgezogen.
- Import-Beleg im Parser von Tag eins: `DAG` ohne airflow-Import → nicht geparst
  (verhindert den 330-Zeilen-Fehler aus 006 im Produkt).
- Konsistenz-Test `scanner/parse_consistency_test.py` pinnt Signal-Arten-Gleichheit
  Parser ↔ Scanner dauerhaft auf den Fixtures.

### Verifiziert

- `pytest`: **256 passed** (42 neue). Übersetzungstabelle als Tests **vor** dem Modul
  (rot → grün im Log belegt), zwei Korpus-Funde ebenfalls erst rot fixiert (ClassDef-Walk,
  A+B-Doppelsignal).
- `ruff check`, `ruff format --check`, `mypy` (33 Files) grün. Kern weiter ohne
  Laufzeit-Dependencies.

## Hinweise für nächste Session

- **Roadmap: 008 (dbt-Parser + Dauern-Schicht)** ist der nächste Schritt. `to_pipeline`
  nimmt bereits ein Dauern-Mapping (Knoten namespaced `dag_id.task_id`), die Dauern-Schicht
  muss es nur füllen.
- **Für den Orchestrator zu prüfen:** (1) Der Teilpfad-Befund ist fast ausschließlich λ = 1
  (131 von 135 Zeilen; einzelne `depends_on_past`-Selbstkante). Trägt der Fall-Katalog für
  den Launch, oder braucht es die λ=2-Fälle als Aushängeschild? (2) Die C-Kanten-Null:
  Vorbehalt ist der Parse-Satz (nur Kandidaten-Files, nicht ganze Repos) — ein Lauf mit
  ganzen Repos als Parse-Satz würde die "Ziel nicht im Parse-Satz"-Fälle (14 von 34)
  auflösen, kostet aber deutlich mehr Zeit. Lohnt das vor 009?
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im **Scanner** (der Parser hat
  ihn schon), DAG-Generatoren mit Literal-Argumenten, ADR-017-Doppelnummer ist seit 006a
  bereinigt (ADR-019).
- Vendorte Airflow-Kopien im Kandidaten-Korpus (mehrere `apache/airflow`-Forks) verzerren
  File-Statistiken; `scan/007_parse/` ist ein Technik-Artefakt, für Marktzahlen bleibt
  `scan/v2/` maßgeblich.

## Was David entscheiden muss

1. Nichts Blockierendes. Launch-Frage aus 006 steht weiter offen (Zahlen unverändert
   zitierfähig); Session 008 kann ohne Entscheidung starten.
