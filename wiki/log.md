# Session-Log

Chronologisch. Neue Einträge unten anhängen. Jeder Eintrag nennt, was gemacht wurde, was gemessen wurde und was überrascht hat.

---

## 000 — Orchestrierung, Doku-Skelett, Prototyp-Verifikation (2026-07-13)

**Rolle:** Orchestrator. Kein Produktiv-Code geschrieben, das ist Absicht.

**Gemacht:**
- Projekt-Skelett: `CLAUDE.md`, `STATUS.md`, `README.md`, `wiki/`, `cc-sessions/`, git-Repo initialisiert.
- Wiki angelegt: `index`, `math`, `signals`, `architecture`, `positioning`, `roadmap`, `decisions`, `log`, `changelog`.
- Session-Specs 001 bis 004 geschrieben.

**Gemessen:**

Der Auftrag beschrieb `maxplus_pipeline.py` als vorhandenen, validierten Prototyp. Zu Sessionbeginn war die Datei auf keiner Maschine auffindbar (Suche über `/home/webse`, `/mnt/data`, `/tmp`). Die Referenzwerte waren damit unbelegt, und ADR-001 stand zunächst als offener Blocker im Wiki. David hat die Datei nachgereicht (`wiki/maxplus_pipeline.py`), zwei Zustellversuche kamen nicht an, der dritte über direktes Ablegen im Ordner hat funktioniert.

Prototyp ausgeführt, Ausgabe:

```
Critical Path eines Laufs (Latenz): 5.5 h
Nachhaltige Zykluszeit: lambda = 4.40 h
Kritischer Kreis (kondensiert): monitor -> monitor
  Segment monitor(k-1) -> monitor(k) via: core -> features -> retrain -> score -> monitor
Drift/Lauf (letzte 5): 1.40 h/Lauf; Theorie lambda - T = 1.40 h/Lauf
(a) Retrain halbieren:            lambda = 3.60 h
(b) Quality-Gate asynchron:       lambda = 2.50 h
(c) Core-Job optimieren:          lambda = 3.85 h
```

λ zusätzlich von Hand nachgerechnet: Cross-Kante `monitor(k-1) → core(k)` speist den Intra-Pfad `core (1.1) + features (0.9) + retrain (1.6) + score (0.5) + monitor (0.3) = 4.4`, Kreislänge 1, Zyklusmittel 4.4. Alle Auftrags-Referenzwerte sind damit reproduziert **und** hergeleitet. ADR-001 aufgelöst.

**Was überrascht hat:**

1. **Der kritische Kreis ist nicht der, der im Auftrag steht.** Der Auftrag nennt `core → features → retrain → score → monitor`. Im kondensierten Graphen ist der Kreis aber die Selbst-Kante `monitor → monitor`, und die genannte Kette ist der aufgelöste Intra-Pfad dieses einen Segments. Beide Beschreibungen sind korrekt, meinen aber verschiedene Objekte. Wer nur eine davon im Report zeigt, produziert Verwirrung. Daraus wurde ADR-002.

2. **Der Prototyp kondensiert nicht wirklich.** `build_Abar` spannt die Matrix über alle acht Jobs auf, nicht nur über die drei Cross-Run-Quellen (`core`, `retrain`, `monitor`). Das Ergebnis stimmt trotzdem, weil Knoten ohne ausgehende Cross-Kante auf keinem Kreis liegen können und deshalb nichts beitragen. Für den Produktions-Code ist die echte Kondensation auf die Cross-Run-Knoten trotzdem richtig, weil Karp mit `O(V·E)` skaliert und V sonst unnötig die Task-Anzahl statt der Cross-Run-Knoten-Anzahl ist.

3. **Der Prototyp kennt keinen Perioden-Versatz.** `CROSS` ist eine Liste von Paaren, der Versatz ist implizit immer 1. Der im Auftrag geforderte Test-Case "Zwei-Perioden-Kreis via `execution_delta = 2 * Periode`" lässt sich mit dieser Datenstruktur nicht ausdrücken. Der Produktions-Datentyp braucht ein Tripel `(von, nach, versatz)`, und der Versatz muss ins Zyklusmittel eingehen (eine Kante mit Versatz n zählt als n Kanten). Das ist die erste echte Erweiterung über den Prototyp hinaus und steht so in Spec 004.
