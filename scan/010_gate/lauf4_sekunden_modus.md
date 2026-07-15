**eigenlag check: ausgeloest** — load_data_wikiviews: neue Cross-Run-Kante und Lambda = 5000 s (83,33 min) ueber dem Takt T = 3600 s (60 min).

### load_data_wikiviews

- Lambda: kein Kreis -> 5000 s (83,33 min) (vorher -> nachher)
- Takt T: 3600 s (60 min), Quelle: Schedule '@hourly'
- Neue Cross-Run-Kanten (5):
  - `load_data_wikiviews.check_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.load_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.load_data -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
- **Ausgeloest:** neue Cross-Run-Kante und Lambda = 5000 s (83,33 min) ueber dem Takt T = 3600 s (60 min)
- **Ausloesende Kante:** `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10)
- Kritischer Kreis, kondensiert: `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file`, Gewicht 5000 s (83,33 min), 1 Periode zurueck [wait_for_downstream, pipeline.py:10]
- Aufgeloest: load_data_wikiviews.load_data -> load_data_wikiviews.create_success_file
- Behebung: Keine einzelne Standard-Aenderung (Kreis-Task halbiert, Cross-Kante entfernt) bringt Lambda unter T; der Kreis traegt an mehreren Stellen dasselbe Zyklusmittel.

---
_Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

