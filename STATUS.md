# STATUS

> Wird am Ende jeder Session überschrieben. Schnelle Orientierung für die nächste Session.

## Stand: Session 013 — Launch-Kit (2026-07-16)

**Alles für Davids Schalter liegt bereit; veröffentlicht hat die Session nichts.**
Der Sofort-Einstieg (`eigenlag demo`), das Demo-GIF, der CI-Workflow, die PyPI-fertigen
Artefakte und die fünf Launch-Texte als redigierbare Entwürfe. Reihenfolge der Schalter:
`launch/launch-checklist.md`.

### Kern-Ergebnisse (Belege in `wiki/log.md`, Session 013; Artefakte in `scan/013_launch/`)

| Was | Ergebnis |
|---|---|
| `eigenlag demo` | Eingebauter Subcommand, voller Report der Prototyp-Pipeline (λ = 4.4 h, T = 3 h) über `compose()`/`render()`, EN/DE, 0,113 s über den pipx-Entry-Point. Fixture zog als Single Source nach `eigenlag/demo.py` |
| 012a-Feinschliff | `plan_fix_task_halved` EN/DE neu, "foreign task" ist raus, README-Sweep erledigt |
| GIF | `assets/demo.gif` aus `launch/build-demo-gif.sh` (vhs + ffmpeg-Nachschritt): 2,1 MB, 14,6 s, ~2 s sichtbares Zeilen-Scrollen, Endframe = Plan-Sektion mit −43,18 %; im README oben eingebettet (Korrektur 013b, wiki/log.md) |
| CI | `.github/workflows/ci.yml` (Matrix 3.12/3.14) = exakt die Frisch-Clone-Probe: `pip install -e ".[db,scanner]" pytest ruff mypy`, dann pytest/ruff/format/mypy. Probe grün (377 passed, frische venv, Temp-Clone) |
| PyPI | `python -m build` + `twine check` PASSED, Name frei (404 geprüft 2026-07-16), Anleitung `docs/pypi-release.md`, README-Umstellung als **nicht angewandter** Patch `launch/readme-pypi-install.patch` |
| Launch-Texte | 5 DRAFTs unter `launch/`: reddit-post, wikimedia-mail, airflow-slack, release-notes-v0.1.0, launch-checklist |

### Verifiziert

- `pytest`: **377 passed** (+7 `demo_test.py`, tests-zuerst). `ruff check`, `ruff format
  --check`, `mypy` (53 Files) grün — lokal UND in der Frisch-Clone-Probe.
- Demo über den Entry-Point in beiden Sprachen gefahren (`scan/013_launch/demo_en.txt`/`_de.txt`).
- GIF-Endframe per ffmpeg extrahiert und gesichtet.

## Hinweise für nächste Session

- **Abnahme 013 durch den Orchestrator steht aus.** Prüfschwerpunkte: die Launch-Texte
  gegen die Schreibregeln (das ist der heikelste Teil), die Demo-Annahmen (MC-Streuung
  p95 = 1,5 × p50 und n = 40 sind deklarierte Beispiel-Annahmen aus dem 012-Artefakt),
  und die CI-Abweichung von der Spec: installiert wird `.[db,scanner]` statt `.[db]`,
  weil die Scanner-Tests pyyaml brauchen — Skip-Marker waren nirgends nötig, kein Test
  braucht Netz/Docker/`data/`.
- **Kein 014-Spec abgelegt:** Die nächsten Schritte sind Davids Schalter
  (`launch/launch-checklist.md`), danach entscheidet das Feedback (positioning.md,
  Trigger-Momente) über die Produktrichtung. Erst dann lohnt ein neuer Spec.
- **Erster echter CI-Lauf** passiert erst nach dem Public-Schalter (Checklisten-Schritt 2);
  das 3.12-Bein war lokal nicht prüfbar (Server hat nur Python 3.14.4).
- GIF-Reproduktion: `bash launch/build-demo-gif.sh` (setzt `VHS_NO_SANDBOX=true` selbst,
  Chromium-Sandbox am Server nicht nutzbar; vhs/ttyd liegen in `~/.local/bin`). vhs 0.11.0
  ignoriert `Set Framerate`, daher der ffmpeg-Nachschritt im Skript.
- **Offen aus 006a (unverändert):** Import-genauer DAG-Check im Scanner, DAG-Generatoren
  mit Literal-Argumenten. **dbt-Parser** bleibt bis nach dem Feedback-Meilenstein vertagt.

## Was David entscheiden muss

1. **Die Schalter**: Repo public → CI prüfen → PyPI → README-Patch → Tag v0.1.0 →
   About/Topics → Pause → Wikimedia-Mail → Reddit → Slack. Jeder Schritt mit Wirkung
   und Risiko in `launch/launch-checklist.md`; nichts davon löst eine Session aus.
2. Die fünf Launch-Texte redigieren — sie sind bewusst "fertig zum Redigieren",
   nicht "fertig zum Posten".
