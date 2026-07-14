# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 006 — Re-Scan mit Zwei-Klassen-Risiko, Fall-Korrektur (2026-07-14)

**Die Korpus-Zahlen sind neu erhoben und damit wieder behauptbar. `scan/v2/` ist der
zitierfähige Stand.** Voller Lauf über die 1692 gecachten Clones (1193 s, kein Neu-Klonen,
kein GitHub-Request), unter der Definition nach ADR-015/016, mit Signal G als eigener Klasse
(ADR-018).

### Die Zahlen

| Größe | 003 | 006 | Einordnung |
|---|---|---|---|
| DAGs gefunden | 51.426 | 51.789 | +363 durch ADR-015, fast alle ohne `dag_id` und signalfrei |
| Cross-Run (A–F) | 1.303 | 1.303 | mengen-identisch, nicht nur zahlengleich |
| **Risiko-Kandidaten (Kern)** | 176 | **176** | dieselben Zeilen; das bleibt die Launch-Zahl |
| Risiko-Kandidaten (nur G) | — | **473** | neue Klasse, 159 Repos; λ = Makespan, dort reicht Laufzeit-Monitoring |
| DAGs ohne `dag_id` | 4.587 | 4.952 | geflaggt, nicht geraten |
| dbt-Models mit Selbst-Kante | 3.369 | 3.369 | byte-identisch übernommen |

**Der überraschende Befund:** Die 005-Hypothese "die Marktzahl steigt deutlich" ist gemessen
widerlegt. Der öffentliche Korpus kapselt seine DAG-Erzeugung kaum; ADR-015 wirkt fast nur bei
professionellen Umgebungen (Wikimedia: 71 → 345 DAGs), und genau die sind in der
Code-Search-Stichprobe unterrepräsentiert. Details und Stichproben:
`scan/v2/sample_verification.md` (3 × 10 Stichproben, 0 Falsch-Positive, 0 Falsch-Negative).

### Was sonst passiert ist

- **ADR-018** (zwei Risiko-Klassen) in `wiki/decisions.md`; `signals.md` entsprechend
  korrigiert. Wäre G still ins STRONG-Set gewandert, wäre die Quote 176 → 649 gesprungen —
  mit Kandidaten, die jedes Dashboard findet. Die Trennung ist die Verteidigung der Zahl.
- **`wikimedia/case.md` nach ADR-017 korrigiert:** Sweep als Überschrift (30 über Takt, 29
  driften nicht), "1,6 Sekunden Reserve" gestrichen (Fixpunkt statt Marge), λ = Laufdauer auf
  DAG-Ebene explizit, wcqs-Ausreißer-Absatz. Messwerte unverändert.
- **`report.py`:** `sig_g_max_active_runs`, `risk_candidate_g_only`, `dag_id_missing`,
  Vorher/Nachher-Tabelle, Offenlegung der Definitionsänderung. Permalinks jetzt URL-encodiert
  (Dateien mit `#` im Namen waren nicht nachschlagbar, Regel-6-Fund aus der Stichprobe).

### Verifiziert

- `pytest`: **214 passed.** `ruff check`, `ruff format --check`, `mypy` (29 Files) grün.
- Gegenprobe Definitionsgleichheit: neues `report.py` auf dem **003-State** reproduziert
  exakt 51.426 / 1.303 / 176 / 0 G-only.
- `scan/v2/scan_dbt.csv` per `diff` byte-identisch mit `scan/scan_dbt.csv`.
- Lauf-Beleg: `data/scan_run_v2.log` ("Fertig: 1692 Repos in 1193s").

## Hinweise für nächste Session

- **Phase 1 ist damit abgeschlossen** (Akzeptanz laut `wiki/roadmap.md`: Launch-Zahlen aus
  `scan/v2/`, mit Nenner, Permalink und offengelegter Definitionsänderung). Nächste Session
  laut Roadmap: **007, Airflow-Parser** (Phase 2). Ein Spec-Entwurf liegt als
  `cc-sessions/007_offen-airflow-parser.md`; er stammt aus der Implementer-Session 006 und
  paraphrasiert nur Roadmap und offene Abnahme-Punkte — der Orchestrator sollte ihn vor
  Start schärfen oder ersetzen.
- **ADR-Kandidat aus der Stichprobe (keine Regel-Änderung in 006, Spec-Grenze):** Die
  repo-weite Namensauflösung von ADR-015 übertreibt bei generischen Methodennamen. 330 der
  422 neuen DAG-Zeilen stammen aus einem generierten OpenAPI-Client, dessen Modellklasse
  `DAG` heißt (`mik-laj/airflow-api-clients`, Methode `make_instance`). Signalfrei, keine
  Quote berührt, aber der Nenner dieses Repos ist aufgebläht. Import-genaue Auflösung wäre
  die Fortsetzung.
- **Weiter offen aus 005:** DAG-Generatoren mit Literal-Argumenten an der Aufrufstelle
  auflösen (90 Wikimedia-DAGs ohne `dag_id`); die zehn Wikimedia-DAGs mit unplausiblen
  Gauge-Wertwechseln (kein λ gerechnet); Task-Ebene braucht die Airflow-Metadaten-DB, also
  einen Kunden.
- Die beiden ADR-017-Einträge in `wiki/decisions.md` tragen dieselbe Nummer (Gauge-Rekonstruktion
  und Fall-These, beide aus 005). Nicht umnummeriert, weil Querverweise auf "ADR-017" in
  mehreren Dokumenten stehen; bei Gelegenheit vom Orchestrator zu bereinigen.

## Was David entscheiden muss

1. **Launch-Zeitpunkt.** Die Marktzahl (176 Kern-Kandidaten, 0,3 %, dazu 1.303 Kreise ohne
   bekannte Taktgrenze) und der Wikimedia-Fall sind jetzt konsistent, belegt und zitierfähig.
   Nichts davon ist veröffentlicht, nichts nach außen gegangen. Ob und wann, entscheidet David.
2. **Session 007 (Parser) starten** oder vorher etwas anderes priorisieren. Der Spec-Entwurf
   liegt, die Roadmap sieht 007 als nächsten Schritt.
