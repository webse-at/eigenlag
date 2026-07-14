import json
from pathlib import Path
from typing import Any

import pytest

from wikimedia.fetch import Fetcher, FetchError


def fetcher(tmp_path: Path, **kwargs: Any) -> Fetcher:
    return Fetcher(cache_dir=tmp_path / "cache", error_log=tmp_path / "errors.jsonl", **kwargs)


def test_offline_ohne_cache_ist_ein_fehler(tmp_path: Path) -> None:
    with pytest.raises(FetchError):
        fetcher(tmp_path, offline=True).query("up")


def test_zweiter_lauf_liest_aus_dem_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live = fetcher(tmp_path)
    result = [{"metric": {}, "value": [1, "2"]}]
    payload: dict[str, Any] = {"status": "success", "data": {"result": result}}
    monkeypatch.setattr(Fetcher, "_get", lambda self, path, params: payload)
    assert live.query("up")

    cached = fetcher(tmp_path, offline=True)  # offline: jeder echte Request wuerde werfen
    assert cached.query("up") == result
    assert cached.cache_hits == 1


def test_leere_serie_wird_protokolliert(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live = fetcher(tmp_path)
    monkeypatch.setattr(
        Fetcher, "_get", lambda self, path, params: {"status": "success", "data": {"result": []}}
    )
    assert live.query("airflow_gibt_es_nicht") == []
    logged = [json.loads(line) for line in (tmp_path / "errors.jsonl").read_text().splitlines()]
    assert logged[0]["error"] == "leere Serie"


def test_query_range_wird_geblockt_und_wieder_zusammengesetzt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Prometheus weist mehr als 11.000 Punkte ab, also zerlegt der Fetcher das Fenster.
    calls: list[dict[str, str]] = []

    def fake(self: Fetcher, path: str, params: dict[str, str]) -> dict[str, Any]:
        calls.append(params)
        start, end = int(params["start"]), int(params["end"])
        return {
            "status": "success",
            "data": {
                "result": [
                    {"metric": {"dag_id": "d"}, "values": [[t, "1"] for t in range(start, end, 60)]}
                ]
            },
        }

    monkeypatch.setattr(Fetcher, "_get", fake)
    live = fetcher(tmp_path)
    result = live.query_range("q", 0, 60 * 25_000, 60)

    assert len(calls) == 3  # 25.000 Punkte, 10.000 je Block
    assert len(result) == 1
    timestamps = [t for t, _ in result[0]["values"]]
    assert timestamps == sorted(set(timestamps))  # Blockgrenzen doppeln keinen Punkt
    assert len(timestamps) == 25_000
