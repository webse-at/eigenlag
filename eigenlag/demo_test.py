"""Tests-zuerst fuer `eigenlag demo` (Spec 013, Auftrag 1).

Der Subcommand rendert den vollen Report der Prototyp-Pipeline (Ground Truth vom
2026-07-13: lambda = 4.40 h bei T = 3.0 h, ADR-001) ueber denselben
compose()/render()-Pfad wie analyze — kein Netz, keine Dateien. Die Kopfzeile
weist das Beispiel als Beispiel aus, die Fusszeile zeigt den naechsten Schritt.
Alle Pins stammen aus dem abgenommenen 012-Artefakt (scan/012_plan/lauf1) und
sind von Hand nachgerechnet: 15840 s = 4.4 h, 9000 s = 2.5 h, -43.18 % = 6840/15840.
"""

from __future__ import annotations

import pytest

from eigenlag.cli import main


def demo_output(capsys: pytest.CaptureFixture[str], *args: str) -> str:
    assert main(["demo", *args]) == 0
    return capsys.readouterr().out


def test_demo_exit_0_und_englisch_ist_default(capsys: pytest.CaptureFixture[str]) -> None:
    out = demo_output(capsys)
    assert "Acceleration plan" in out
    assert "Verdict" in out


def test_demo_kopfzeile_weist_das_beispiel_aus(capsys: pytest.CaptureFixture[str]) -> None:
    out = demo_output(capsys)
    assert out.startswith("Built-in example")
    de = demo_output(capsys, "--lang", "de")
    assert de.startswith("Eingebautes Beispiel")


def test_demo_fusszeile_zeigt_den_naechsten_schritt(capsys: pytest.CaptureFixture[str]) -> None:
    schritt = "eigenlag analyze your/dags --assume-duration 300"
    assert schritt in demo_output(capsys)
    assert schritt in demo_output(capsys, "--lang", "de")


def test_demo_pins_der_prototyp_ground_truth(capsys: pytest.CaptureFixture[str]) -> None:
    out = demo_output(capsys)
    assert "15840 s (4.4 h)" in out  # lambda, Prototyp-Lauf 2026-07-13
    assert "10800 s (3 h)" in out  # T = 3.0 h
    assert "9000 s (2.5 h)" in out  # Kante monitor -> core entfernt
    assert "-43.18 %" in out  # 6840/15840, von Hand geprueft (Abnahme 012a)


def test_demo_plan_traegt_den_tragfaehigkeits_marker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert "makes your current schedule sustainable" in demo_output(capsys)
    assert "macht den laufenden Takt tragfaehig" in demo_output(capsys, "--lang", "de")


def test_demo_deutsch_rendert_den_plan(capsys: pytest.CaptureFixture[str]) -> None:
    out = demo_output(capsys, "--lang", "de")
    assert "Beschleunigungsplan" in out
    assert "Urteil" in out


def test_kein_foreign_task_mehr_im_katalogtext(capsys: pytest.CaptureFixture[str]) -> None:
    # 012a-Feinschliff: der Deutschismus "foreign task" ist ersetzt, die Entscheidung
    # ueber den Schnitt bleibt beim Leser.
    out = demo_output(capsys)
    assert "foreign task" not in out
    assert "yours to judge" in out
