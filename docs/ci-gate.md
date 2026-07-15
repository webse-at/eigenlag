# CI-Gate: `eigenlag check --against REF`

Das Gate vergleicht λ und die Cross-Run-Kanten-Menge des Arbeitsstands gegen einen
Git-Stand (`REF`, typisch `origin/main`) und schlägt an, bevor eine Änderung gemerged
ist, die die Pipeline über ihre Taktgrenze hebt.

```
eigenlag check PFAD --against REF
  [--db URL | --assume-duration SEK]     sonst Struktur-Modus
  [--dag-id ID] [--period SEK] [--statistic mean|p50|p95] [--since TAGE]
  [--fail-on-new-edge] [--max-increase PROZENT]
  [--comment-file PFAD] [--json]
```

## Exit-Codes

| Code | Bedeutung |
|---|---|
| 0 | bestanden — auch: keine DAGs in beiden Ständen (mit Hinweis) |
| 1 | Bedienfehler (Pfad fehlt, REF nicht auflösbar, kein Git-Repo, unbekannte `--dag-id`) |
| 3 | Gate ausgelöst |

Die 2 bleibt bei `analyze` reserviert; die Räume überlappen nicht.

## Fail-Regeln

**Default (nach Auftrag):** Exit 3, wenn eine neue Cross-Run-Kante hinzukam **und**
λ_nachher über dem Takt T liegt (T aus dem Schedule oder `--period`). Mit
`--db`/`--assume-duration` ist λ in Sekunden und der Vergleich wörtlich. Ohne
Dauern-Quelle (Struktur-Modus, der CI-Default) ist λ in Task-Einheiten nicht gegen
Sekunden vergleichbar; dann löst eine neue Kante aus, die einen Kreis über die
Zeitachse schließt, bei bekanntem sub-täglichem Takt (ADR-022).

**Schärfere Modi:**

- `--fail-on-new-edge`: jede neue Cross-Run-Kante löst aus, unabhängig von T — für
  Teams, die Kanten bewusst budgetieren.
- `--max-increase PROZENT`: deckelt das λ-Wachstum gegenüber `REF`, auch ohne neue
  Kante (etwa wenn ein Task in den kritischen Kreis rückt).

Gate-Metrik ist Punkt-λ gegen Punkt-λ auf derselben Statistik (ADR-022). Monte Carlo
läuft nie gegen Schwellen.

## Mechanik

Der Vergleichsstand kommt aus einem temporären detached Worktree
(`git worktree add --detach`), derselbe relative Pfad wird auf beiden Ständen geparst,
danach wird der Worktree entfernt — auch bei Fehlern. Kein Checkout im Nutzer-Tree,
keine Mutation am Arbeits-Repo, kein Netz. Der PR-Kommentar geht als Markdown auf
stdout bzw. mit `--comment-file` in eine Datei; **das Tool postet nie selbst** — das
Posten ist Sache des CI-Jobs.

## GitHub-Actions-Beispiel

```yaml
name: eigenlag-gate

on:
  pull_request:
    paths:
      - "dags/**"

jobs:
  check:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # das Gate braucht die Basis-Referenz im Clone

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: eigenlag installieren
        run: pip install eigenlag  # oder: pip install git+<repo-url>

      - name: Gate laufen lassen
        id: gate
        run: |
          eigenlag check dags --against "origin/${{ github.base_ref }}" \
            --comment-file kommentar.md

      - name: PR-Kommentar posten
        if: always() && steps.gate.outcome != 'skipped'
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          path: kommentar.md
```

Der `check`-Schritt bricht bei Exit 3 den Job ab; der Kommentar-Schritt läuft mit
`if: always()` trotzdem und postet den Text aus `kommentar.md`. Mit einer
Metadaten-DB, die aus der CI erreichbar ist, wird aus dem Struktur-Vergleich eine
Zeit-Aussage: `--db "$AIRFLOW_DB_URL"` anhängen.
