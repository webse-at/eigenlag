**eigenlag check: bestanden.** Keine Aenderung hebt Lambda ueber den Takt (`/tmp/claude-1000/-mnt-data-projects-eigenlag/43709c11-0215-4778-b8c2-393860dac021/scratchpad/fixture-repo/repo/dags` gegen `v3`).

Struktur-Vergleich: Lambda in Task-Einheiten (uniforme Dauer 1.0 je Task, keine Dauern-Quelle angegeben). Fuer Lambda in Sekunden gegen den Takt: --db oder --assume-duration.

1 DAG(s) ohne Aenderung an Cross-Run-Kanten oder Lambda.

---
_Lambda ist eine Untergrenze der realen Taktzeit: unbegrenzte Parallelitaet ist angenommen. Retries, Sensor-Poking und Pool-Limits sind nicht modelliert; sie koennen die reale Taktzeit nur erhoehen, nie senken._

