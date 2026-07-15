**eigenlag check: ausgeloest** — load_data_wikiviews: neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse bei sub-taeglichem Takt (T = 3600 s (60 min)).

Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder --assume-duration.

### load_data_wikiviews

- Lambda: kein Kreis -> 2 Task-Einheiten (vorher -> nachher)
- Takt T: 3600 s (60 min), Quelle: Schedule '@hourly'
- Neue Cross-Run-Kanten (5):
  - `load_data_wikiviews.check_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.load_data -> load_data_wikiviews.check_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.load_data -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
  - `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file` (wait_for_downstream, pipeline.py:10, 1 Periode zurueck)
- **Ausgeloest:** neue Cross-Run-Kante schliesst einen Kreis ueber die Zeitachse bei sub-taeglichem Takt (T = 3600 s (60 min))
- **Ausloesende Kante:** `load_data_wikiviews.create_success_file -> load_data_wikiviews.load_data` (wait_for_downstream, pipeline.py:10)
- Kritischer Kreis, kondensiert: `load_data_wikiviews.create_success_file -> load_data_wikiviews.create_success_file`, Gewicht 2 Task-Einheiten, 1 Periode zurueck [wait_for_downstream, pipeline.py:10]
- Aufgeloest: load_data_wikiviews.load_data -> load_data_wikiviews.create_success_file
- Behebung: Die ausloesende Kante zu entfernen behebt den Fail. Eine Zeit-Aussage (Lambda gegen T in Sekunden) braucht eine Dauern-Quelle: --db oder --assume-duration.

---
_Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

