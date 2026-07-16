# Session 012 — Beschleunigungsplan: aus der Diagnose wird das Produkt

**Die Produkt-Session.** Bisher sagt der Report „hier ist deine Grenze und wer schuld ist". Nach dieser Session sagt er: **„Deine Pipeline könnte alle X laufen statt alle Y. Hier ist die Änderung, die den Unterschied kauft, und hier ist, was sie dir bringt."** Jeder Befund wird als unbeanspruchte Reserve formuliert, nicht als Mangel. Die konzeptionelle Grundlage steht in `wiki/positioning.md` (Trigger-Momente, Produkt-Ebene) — vor dem Implementieren lesen.

## Vorher lesen

- `wiki/positioning.md`, Abschnitte "Zwischenbewertung nach Phase 1" und "Der Use-Case: wann der verborgene Schmerz akut wird"
- `wiki/signals.md` (die Kanten-Arten — der Behebungs-Katalog hängt an ihnen)
- `eigenlag/report.py`, `eigenlag/messages.py` (Katalog-Mechanik aus 011), `wiki/decisions.md` ADR-020, ADR-023
- Abnahme-Einträge 009a–011a in `wiki/log.md`

## Vorentschieden (Orchestrator, nicht neu verhandeln)

1. **Kein `--value-per-hour`-Flag.** Das war im Gespräch mit David angedacht, ich streiche es bewusst (David weiß es, Veto-Recht liegt bei ihm): Eine Euro-Rechnung im Tool braucht eine Einheit wie "€ pro Stunde Datenfrische pro Tag", und an genau solchen verwirrenden Kunstgrößen kippt die Glaubwürdigkeit bei einer skeptischen Zielgruppe. Das Tool liefert die **physikalischen** Gewinnzahlen (Läufe pro Tag, Frische-Delta, weggeräumte Verspätung); die Übersetzung in Geld machen die Marketing-Texte (Session 013) in Prosa-Beispielen. Der Report bleibt ein Messinstrument, der Produkt-Charakter kommt aus der Handlungsorientierung, nicht aus einer Pseudo-Währung.
2. **Der Behebungs-Katalog ist Muster-Wissen, kein Orakel.** Je Kanten-Art (A, B, C, D, G, dbt-E) ein Behebungs-Muster in beiden Sprachen, mit ehrlicher Vorbedingung. Formulierung als "üblicher Weg" ("commonly resolved by …"), nie als Garantie. Der Katalog lebt in der Nachrichten-Katalog-Mechanik aus 011 (`messages.py` oder Schwester-Modul), der Vollständigkeits-Test wird erweitert: jede Kanten-Art hat ein Muster, jedes Muster existiert in EN und DE.
3. **Der Plan zeigt nur, was λ senkt.** Aktionen mit Delta null bleiben in der Sammelzeile (010-Korrektur gilt weiter). Berechnet werden wie bisher zwei Aktions-Typen: Kante entfernen (mit Behebungs-Muster aus dem Katalog) und Task halbieren (generische Optimierung). Nutzer-Szenarien via `--what-if` bleiben unverändert und erscheinen immer.
4. **Zwei Gewinn-Formulierungen, je nach Urteil, mit exakt definierter Rechnung:**
   - **Instabil (λ > T):** Der Gewinn einer Aktion ist, dass sie den bestehenden Takt tragfähig macht: "makes your current schedule sustainable" gilt genau dann, wenn λ_neu < T. Zusätzlich beziffert: die weggeräumte Drift (λ − T pro Lauf) bzw. bei λ ≈ T die stehende Verspätung. Keine Aktion schafft λ_neu < T → der Plan sagt auch das ehrlich ("no single change makes T sustainable; the two cheapest in combination: …" — Kombinationen nur als Paar-Rechnung der Top-3-Aktionen, nicht kombinatorisch explodieren lassen).
   - **Stabil (λ < T):** Der Gewinn ist Headroom: "floor drops from λ to λ_neu; a schedule of λ_neu instead of T would run N more times per day (24h/λ_neu − 24h/T) and keep data up to (T − λ_neu) fresher at any moment." Das "up to" ist Pflicht, und die Fußzeile sagt, dass λ eine Untergrenze ohne Betriebsreserve ist — wir empfehlen keinen konkreten neuen Schedule, wir beziffern die Reserve (Regel 2: keine erfundene Marge, also auch kein "λ × 1,25 wäre sicher").
5. **Report-Reihenfolge neu:** Urteil → kritischer Kreis (kompakt, wie gehabt) → **Beschleunigungsplan** (das Herzstück, ersetzt die What-if-Sektion) → Monte Carlo → Warnungen. Die JSON-Ausgabe bekommt **additive** Felder (Aktionsliste mit Kanten-Art, Katalog-Schlüssel, λ_neu, Delta absolut und Prozent, Gewinn-Feldern); bestehende Keys bleiben eingefroren (ADR-023-Politik). Das Ganze wird **ADR-024**.

## Auftrag

### 1. Behebungs-Katalog

Je Kanten-Art ein Eintrag (EN und DE), Ton ruhig und konkret, mit Vorbedingung:

| Kanten-Art | Muster (sinngemäß, Formulierung ist Feinarbeit der Session) |
|---|---|
| A `depends_on_past` | Prüfen, ob der Task den Vorlauf braucht oder nur Reihenfolge will; idempotente, partitionierte Inkremente brauchen ihn oft nicht |
| B `wait_for_downstream` | Schützt meist vor überlappenden Writes; Partitions-Isolation (jeder Lauf schreibt eigene Partition) erlaubt Überlappung |
| C Sensor mit `execution_delta` | Polling durch Dataset-/Asset-Trigger ersetzen oder prüfen, ob der Versatz kleiner sein kann |
| D `include_prior_dates` | Meist eine Vorsichts-Einstellung; prüfen, ob der konkrete Vorlauf wirklich gebraucht wird |
| G `max_active_runs=1` | Oft eine Pauschal-Sicherung; wenn Läufe partitions-isoliert schreiben, ist Überlappung sicher |
| Task auf dem Kreis (Halbierung) | Generisch: Task teilen, Inkrement verkleinern, Warm-Start — der Report nennt hier nur die Rechnung, kein erfundenes Detailwissen über fremde Tasks |

Der Katalog behauptet nie, die Änderung sei sicher — er sagt, was üblicherweise geprüft wird. Ein Satz pro Muster, maximal zwei.

### 2. Plan-Berechnung und Rendering

- Aktionsliste wie bisher aus dem What-if-Kern, plus Katalog-Text je Kanten-Aktion, plus Gewinn-Zeile nach Vorentscheidung 4.
- Der Fall "keine Aktion rettet T" (instabil) mit der Paar-Rechnung der Top-3.
- Beide Sprachen, `--json` additiv, Byte-Identität EN/DE bleibt (Test läuft schon, muss grün bleiben).

### 3. Test-Pins (Tests zuerst, die Demo liefert sie frei Haus)

Die Demo-Pipeline (λ = 4.40, T = 3.0, instabil) ist der perfekte Pin, weil sie die Verkaufsgeschichte **ist**:

- Aktion "Kante `monitor → core` entfernen" (Quality-Gate asynchron): λ_neu = 2.50 → **einzige Einzel-Aktion mit λ_neu < T**, der Plan markiert sie als "makes your current schedule sustainable", Delta −43 %.
- Aktion "retrain halbieren" (das GPU-Upgrade): λ_neu = 3.60 → **rettet den Takt nicht** (3.60 > 3.0), und der Plan sagt das.
- Aktion "core halbieren": λ_neu = 3.85 → dito.
- Stabiler Fall (Flaggschiff mit assume): Headroom-Formulierung mit Läufe-pro-Tag- und Frische-Zeile, von Hand nachgerechnet als Pin.
- Kein Kreis → kein Plan, sondern der bestehende "nicht anwendbar"-Pfad (nicht anfassen).

### 4. Verifikation

1. Demo-Pipeline als CLI-Lauf, voller EN-Report ins Log — das ist künftig **das** Marketing-Artefakt: GPU-Upgrade rettet den Takt nicht, die kostenlose Architektur-Änderung schon.
2. Flaggschiff (`load_data_wikiviews`, assume 300) EN und DE, voller Report ins Log.
3. Ein instabiler Fall, bei dem **keine** Einzel-Aktion reicht (synthetische Fixture) → Paar-Rechnung sichtbar.
4. `pipx install --force .`, Läufe über den Entry-Point.

## Akzeptanz

- Alle Pins aus Abschnitt 3 grün, zuerst rot gesehen
- Katalog-Vollständigkeit (jede Kanten-Art × beide Sprachen) per Test erzwungen
- `--json`: additive Felder, bestehende Keys byte-stabil, EN/DE-Identität grün
- ADR-024 in `wiki/decisions.md`, `wiki/positioning.md` um einen Verweis ergänzt (nicht dupliziert — Single Source)
- README-Quickstart-Beispiel zeigt den neuen Plan-Abschnitt (der Output dort ist ein echter, aktualisierter Lauf)
- `pytest`, `ruff`, `mypy` grün; Pflicht-Dependencies weiterhin null

## Explizit nicht in dieser Session

Launch-Texte, Demo-Befehl, CI-Workflow, PyPI, GIF (alles Session 013). Keine Euro-Rechnung im Tool (Vorentscheidung 1). Keine neuen Signal-Arten, keine Empfehlung konkreter neuer Schedules (nur Reserve beziffern, keine Marge erfinden).
