from wikimedia.runs import Run, extract_runs, samples, stats, windows


def series(*points: tuple[float, float]) -> list[dict[str, object]]:
    return [{"metric": {}, "values": [[t, str(v)] for t, v in points]}]


def test_gleicher_wert_hintereinander_ist_ein_lauf() -> None:
    # Die Gauge haelt den Wert ueber mehrere Scrapes: drei Samples, ein Lauf.
    runs = extract_runs(series((100, 3_600_000.0), (160, 3_600_000.0), (220, 3_600_000.0)))
    assert runs == [Run(at=100, duration=3600.0)]


def test_wertwechsel_ist_ein_neuer_lauf() -> None:
    runs = extract_runs(series((100, 3_600_000.0), (160, 3_600_000.0), (3820, 3_720_000.0)))
    assert runs == [Run(at=100, duration=3600.0), Run(at=3820, duration=3720.0)]


def test_zurueckkehrender_wert_zaehlt_wieder() -> None:
    # a, b, a: der dritte Lauf hat zufaellig die Dauer des ersten, ist aber ein eigener Lauf.
    runs = extract_runs(series((0, 1000.0), (60, 2000.0), (120, 1000.0)))
    assert [r.at for r in runs] == [0, 60, 120]


def test_mehrere_pods_werden_zusammengefuehrt() -> None:
    # Der StatsD-Pod wechselt: zwei Serien, eine Zeitachse.
    both = series((0, 1000.0))[0], series((60, 2000.0))[0]
    assert samples(list(both)) == [(0.0, 1000.0), (60.0, 2000.0)]


def test_luecke_trennt_die_fenster() -> None:
    runs = [Run(at=0, duration=60), Run(at=3600, duration=60), Run(at=100_000, duration=60)]
    result = windows(runs, outage_gap=4 * 3600)
    assert [len(w.runs) for w in result] == [2, 1]
    assert result[0].cadence == 3600
    assert result[1].cadence is None  # Ein Lauf allein hat keinen Takt.


def test_takt_ist_der_mittlere_abstand_der_laufenden() -> None:
    runs = [Run(at=0, duration=1), Run(at=3720, duration=1), Run(at=7440, duration=1)]
    window = windows(runs)[0]
    assert window.cadence == 3720
    assert window.span == 7440


def test_stats_p95_liegt_auf_einem_echten_wert() -> None:
    result = stats([float(x) for x in range(1, 101)])
    assert result.n == 100
    assert result.median == 50.5
    assert result.p95 == 95.0
    assert result.minimum == 1.0
    assert result.maximum == 100.0


def test_zwei_pods_gleichzeitig_erzeugen_keine_scheinlaeufe() -> None:
    # Jeder Pod haelt seinen eigenen letzten Wert. Uebereinandergelegt springt die Reihe
    # zwischen 1000 und 2000 hin und her -- das sind zwei Laeufe, nicht sechs.
    pod_a = {"metric": {"pod": "a"}, "values": [[0, "1000"], [60, "1000"], [120, "1000"]]}
    pod_b = {"metric": {"pod": "b"}, "values": [[30, "2000"], [90, "2000"], [150, "2000"]]}
    runs = extract_runs([pod_a, pod_b])
    assert [(r.at, r.duration) for r in runs] == [(0.0, 1.0), (30.0, 2.0)]


def test_derselbe_lauf_von_zwei_pods_zaehlt_einmal() -> None:
    pod_a = {"metric": {"pod": "a"}, "values": [[0, "3600000"]]}
    pod_b = {"metric": {"pod": "b"}, "values": [[60, "3600000"]]}
    assert len(extract_runs([pod_a, pod_b])) == 1


def test_pods_nacheinander_ergeben_eine_durchgehende_folge() -> None:
    alt = {"metric": {"pod": "a"}, "values": [[0, "1000"], [3600, "2000"]]}
    neu = {"metric": {"pod": "b"}, "values": [[7200, "3000"], [10800, "4000"]]}
    assert [r.duration for r in extract_runs([alt, neu])] == [1.0, 2.0, 3.0, 4.0]
