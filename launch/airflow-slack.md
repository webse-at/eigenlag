# DRAFT — David redigiert. Nicht senden.

Ziel: Apache-Airflow-Community-Slack (apache-airflow.slack.com). Kanal vor dem
Posten prüfen: ein Kanal für Community-Inhalte/Show-and-Tell, nicht der
Support-Kanal. Selbst-Autorenschaft ist im Text offen.

---

I measured 30 of Wikimedia's production DAGs whose median runtime exceeds their schedule interval, from their public code and metrics. 29 of them are fine because their runs overlap; what sets the real case apart is an edge across runs. The clearest example is an hourly reconcile DAG that sits exactly on its cycle limit and incurs a constant 48-minute delay as the price of its steady state. I wrote the analysis up with a permalink for every number, and built a small open-source CLI that computes the sustainable cycle-time bound from DAG files: https://github.com/webse-at/eigenlag. Corrections welcome.
