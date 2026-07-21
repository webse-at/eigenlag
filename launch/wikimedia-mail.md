# DRAFT — David redigiert. Nicht senden.

Empfänger (verifiziert 2026-07-21 gegen wikitech.wikimedia.org/wiki/Data_Engineering/Contact):
**data-engineering@wikimedia.org** — die direkte Team-Adresse, ausdrücklich für externe
Anfragen genannt. Keine Anmeldung, kein Account nötig, normale Mail von Davids Adresse.
(Alternativen wären die analytics-Mailingliste — braucht Subscription — oder Phabricator
— braucht Account; beides unnötig umständlich für ein Heads-up.)

Unter 200 Wörter, kein Pitch. Der Repo-Link funktioniert erst, wenn das Repo
public ist (Checkliste beachten).

---

Subject: Analysis of your public Airflow metrics — heads-up and request for corrections

Hello,

I analyzed the scheduling behaviour of your production Airflow instances, using only public sources: the DAG code on your GitLab and the run-duration metrics in your anonymously queryable Prometheus. The full case study, with the PromQL and a commit-pinned permalink for every number, is here:

https://github.com/webse-at/eigenlag/blob/main/wikimedia/case.md

Two findings might interest you:

1. `wdqs_streaming_updater_reconcile_hourly` sits in a stable equilibrium at its cycle limit: over 398 runs, with a mean duration of 3598.4 s against a 3600 s interval, with a constant median schedule delay of 48 minutes. Nothing looks broken; it is the steady state of a run-to-run feedback loop, and the 48 minutes are its price. Whether that price matters for reconciliation is your call.

2. Of 30 DAGs whose median runtime exceeds their schedule interval, 29 do not fall behind, because their runs may overlap. Runtime versus schedule alone would be a poor alert.

I plan to write publicly about this and wanted you to see it first. If any number or reading is wrong, I would like to correct it before that.

Thank you for keeping this data public. That is rare, and it made this analysis possible.

David Paci
