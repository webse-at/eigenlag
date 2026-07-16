# PyPI-Release: Schritt für Schritt

Runbook für den ersten Upload von `eigenlag` auf PyPI. Der Name ist frei
(geprüft 2026-07-16, HTTP 404 auf `pypi.org/pypi/eigenlag/json`). Die Artefakte
sind gebaut und geprüft (`python -m build`, `twine check dist/*` PASSED,
Session 013). Reihenfolge und Kontext: `launch/launch-checklist.md`.

## Voraussetzungen (einmalig)

1. **Account** auf [pypi.org](https://pypi.org/account/register/) anlegen.
   2FA ist Pflicht (TOTP-App reicht).
2. **API-Token** erzeugen: Account Settings → API tokens → "Add API token",
   Scope zunächst "Entire account" (nach dem ersten Upload durch ein
   projekt-gebundenes Token für `eigenlag` ersetzen, altes Token löschen).
   Das Token beginnt mit `pypi-` und wird genau einmal angezeigt.
3. Token lokal ablegen, nicht ins Repo: `~/.pypirc` mit Rechten `600`:

   ```ini
   [pypi]
   username = __token__
   password = pypi-...
   ```

## Upload

Im Repo-Root, auf dem getaggten Stand, mit frisch gebauten Artefakten:

```bash
.venv/bin/python -m build          # baut dist/eigenlag-0.1.0.tar.gz + .whl neu
.venv/bin/twine check dist/*       # muss PASSED zeigen
.venv/bin/twine upload dist/*      # der eigentliche Upload; fragt ohne ~/.pypirc nach dem Token
```

Danach prüfen: `https://pypi.org/project/eigenlag/` zeigt 0.1.0, und auf einer
sauberen Maschine (oder in einer Temp-venv) funktioniert
`pipx install eigenlag && eigenlag demo`.

## Nach dem Upload: README umstellen

Die Install-Zeile im README zeigt bis zum Upload auf `git+https`, weil
`pipx install eigenlag` vorher eine Lüge wäre. Der vorbereitete Patch liegt
unter `launch/readme-pypi-install.patch` und wird **erst nach erfolgreichem
Upload** angewandt:

```bash
git apply launch/readme-pypi-install.patch
git add README.md && git commit -m "README: Install-Zeile auf PyPI umgestellt"
git push
```

Hinweis fürs PyPI-Rendering: das Demo-GIF ist im README relativ eingebettet
(`assets/demo.gif`) und rendert auf pypi.org nicht. Optional im selben Zug auf
die absolute URL umstellen
(`https://raw.githubusercontent.com/webse-at/eigenlag/main/assets/demo.gif`),
sobald das Repo public ist; auf GitHub ändert das nichts.

## Wenn etwas schiefgeht

- **403 beim Upload**: Token falsch kopiert oder Scope zu eng.
- **400 "File already exists"**: eine Version kann nie überschrieben werden.
  Fix einbauen, Version in `pyproject.toml` erhöhen (0.1.1), neu bauen, neu
  hochladen. Es gibt kein "Ersetzen" auf PyPI.
- **README kaputt gerendert auf PyPI**: `twine check` hat es lokal geprüft;
  wenn trotzdem etwas hakt, liegt es meist an relativen Links oder HTML.
