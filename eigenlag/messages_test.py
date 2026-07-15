"""Tests-zuerst fuer die Nachrichten-Kataloge (Spec 011).

Der Vollstaendigkeits-Test ist die Leitplanke der Zweisprachigkeit: jeder Key
existiert in beiden Sprachen, es gibt keinen stillen Fallback auf die jeweils
andere. Ein fehlender Key ist ein KeyError beim Rendern, kein deutscher Satz im
englischen Report (ADR-023).
"""

from __future__ import annotations

import pytest

from eigenlag.messages import CATALOG, dur, fmt, t


def test_beide_kataloge_haben_exakt_dieselben_keys() -> None:
    en = set(CATALOG["en"])
    de = set(CATALOG["de"])
    assert en == de, f"nur EN: {en - de}, nur DE: {de - en}"


def test_kein_katalog_ist_leer() -> None:
    assert CATALOG["en"] and CATALOG["de"]


def test_kein_stiller_fallback_unbekannter_key_wirft() -> None:
    with pytest.raises(KeyError):
        t("en", "gibt_es_nicht")


def test_zahlformat_ist_sprachabhaengig() -> None:
    assert fmt(4.4, "de") == "4,4"
    assert fmt(4.4, "en") == "4.4"
    assert fmt(5000.0, "de") == "5000"
    assert fmt(5000.0, "en") == "5000"


def test_dauer_einheiten_gleich_dezimaltrenner_verschieden() -> None:
    assert dur(3600.0, "de") == "3600 s (60 min)"
    assert dur(3600.0, "en") == "3600 s (60 min)"
    assert dur(18000.0, "de") == "18000 s (5 h)"
    assert dur(90.0, "en") == "90 s"


def test_t_interpoliert_benannte_platzhalter() -> None:
    # Ein Key mit Platzhalter, in beiden Sprachen: die Interpolation greift.
    assert "5" in t("en", "sammel_kopf_n", n=5)
    assert "5" in t("de", "sammel_kopf_n", n=5)
