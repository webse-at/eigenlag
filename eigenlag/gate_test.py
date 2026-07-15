"""Tests-zuerst fuer das CI-Gate (Spec 010).

Exit-Codes sind Vertrag: 0 = bestanden (auch: keine DAGs in beiden Staenden),
1 = Bedienfehler, 3 = Gate ausgeloest. Die 2 bleibt bei `analyze` reserviert.

Die Fixture-Repos haben echte Git-Historie (git init, Commits, Tags) und die
DAG-Files sind gekuerzte Varianten des Flaggschiff-Files aus 009 (echte Struktur,
kontrollierte Historie): wait_for_downstream in default_args, PythonSensor,
BashOperator, Task-Kette.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from eigenlag import gate
from eigenlag.cli import main

V1 = """\
import datetime as dt

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

default_args = {
    "retries": 10,
    "retry_delay": dt.timedelta(seconds=10),
}

dag = DAG(
    dag_id="load_data_wikiviews",
    default_args=default_args,
    start_date=pendulum.datetime(2020, 1, 1),
    schedule_interval="@hourly",
)

check_data = PythonSensor(
    task_id="check_data", python_callable=lambda: True, mode="reschedule", dag=dag
)
load_data = PythonOperator(task_id="load_data", python_callable=lambda: None, dag=dag)
create_success_file = BashOperator(
    task_id="create_success_file", bash_command="touch done", dag=dag
)

check_data >> load_data >> create_success_file
"""

# v2: die eine Zeile, um die es im PR geht — wait_for_downstream in default_args,
# exakt wie im Flaggschiff (dags/wikiviews/load_data.py:49).
V2 = V1.replace('"retries": 10,', '"wait_for_downstream": True,\n    "retries": 10,')
V2_ZEILE = V2.splitlines().index('    "wait_for_downstream": True,') + 1

# Variante mit genau einer neuen Kante: depends_on_past an einem einzelnen Task.
V2_DOP = V1.replace(
    'task_id="load_data", python_callable=lambda: None',
    'task_id="load_data", python_callable=lambda: None, depends_on_past=True',
)

V1_TAEGLICH = V1.replace("@hourly", "@daily")
V2_TAEGLICH = V2.replace("@hourly", "@daily")

# Selbst-Referenz-Sensor (ADR-021): dieselbe Cross-Kante in beiden Staenden,
# aber der Intra-Pfad im Kreis waechst — Lambda steigt ohne neue Kante.
ETS_KURZ = """\
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(dag_id="reconcile", schedule="@hourly") as dag:
    warte = ExternalTaskSensor(
        task_id="warte",
        external_dag_id="reconcile",
        external_task_id="zuletzt",
        execution_delta=dt.timedelta(hours=1),
    )
    zuletzt = PythonOperator(task_id="zuletzt", python_callable=lambda: None)
    warte >> zuletzt
"""

ETS_LANG = ETS_KURZ.replace(
    "    warte >> zuletzt\n",
    '    mitte = PythonOperator(task_id="mitte", python_callable=lambda: None)\n'
    "    warte >> mitte >> zuletzt\n",
)

KEIN_DAG = "WERT = 42\n"


def git(repo: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.strip()


def repo_mit(tmp_path: Path, staende: list[dict[str, str | None]]) -> Path:
    """Fixture-Repo mit echter Historie: je Stand ein Commit, getaggt v1, v2, ..."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    for i, files in enumerate(staende, start=1):
        for name, content in files.items():
            target = repo / name
            if content is None:
                target.unlink()
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
        git(repo, "add", "-A")
        git(
            repo,
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "--no-verify",
            "-m",
            f"v{i}",
        )
        git(repo, "tag", f"v{i}")
    return repo


# --- Default-Fail-Regel und die drei Historien-Staende ----------------------------------


def test_neue_kante_bei_subtaeglichem_takt_loest_aus_mit_datei_und_zeile(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    out = capsys.readouterr().out
    assert code == 3
    assert "ausgeloest" in out
    assert f"pipeline.py:{V2_ZEILE}" in out
    assert "wait_for_downstream" in out


def test_kante_wieder_entfernt_besteht(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = repo_mit(
        tmp_path,
        [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}, {"dags/pipeline.py": V1}],
    )
    code = main(["check", str(repo / "dags"), "--against", "v2"])
    assert code == 0
    assert "bestanden" in capsys.readouterr().out


def test_unveraenderter_stand_besteht(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    assert code == 0
    assert "bestanden" in capsys.readouterr().out


def test_geloeschter_dag_wird_benannt_und_besteht(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V2}, {"dags/pipeline.py": None}])
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    out = capsys.readouterr().out
    assert code == 0
    assert "load_data_wikiviews" in out
    assert "geloescht" in out


def test_neuer_dag_mit_kreis_ist_fail_und_vorher_existierte_nicht(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(
        tmp_path,
        [{"dags/util.py": KEIN_DAG}, {"dags/util.py": KEIN_DAG, "dags/pipeline.py": V2}],
    )
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    out = capsys.readouterr().out
    assert code == 3
    assert "existierte nicht" in out


def test_arbeitsstand_ungecommittet_zaehlt_als_nachher(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}])
    (repo / "dags/pipeline.py").write_text(V2)  # nicht committet
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    assert code == 3
    capsys.readouterr()


def test_taeglicher_takt_besteht_im_struktur_modus(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Struktur-Modus prueft die neue Kante gegen einen sub-taeglichen Takt (ADR-022);
    # @daily ist keiner, also kein Fail nach Default-Regel.
    repo = repo_mit(
        tmp_path, [{"dags/pipeline.py": V1_TAEGLICH}, {"dags/pipeline.py": V2_TAEGLICH}]
    )
    code = main(["check", str(repo / "dags"), "--against", "v1"])
    assert code == 0
    capsys.readouterr()


# --- Sekunden-Modus: Lambda > T woertlich nach Auftrag ----------------------------------


def test_sekunden_modus_neue_kante_und_lambda_ueber_t_loest_aus(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1", "--assume-duration", "2500"])
    out = capsys.readouterr().out
    assert code == 3  # Lambda = 5000 s > T = 3600 s
    assert "5000" in out


def test_sekunden_modus_neue_kante_unter_t_besteht(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1", "--assume-duration", "1500"])
    assert code == 0  # Lambda = 3000 s < T = 3600 s, neue Kante allein reicht nicht
    capsys.readouterr()


def test_behebungs_hinweis_nennt_die_billigste_aenderung_unter_t(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2_DOP}])
    code = main(["check", str(repo / "dags"), "--against", "v1", "--assume-duration", "5000"])
    out = capsys.readouterr().out
    assert code == 3
    assert "Behebung" in out
    assert "halbiert" in out  # load_data halbiert: Lambda 2500 s < T 3600 s


def test_behebungs_hinweis_sagt_wenn_keine_aenderung_reicht(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # V2: mehrere Kreise mit gleichem Zyklusmittel — keine einzelne Standard-Aenderung
    # drueckt Lambda unter T.
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1", "--assume-duration", "2500"])
    out = capsys.readouterr().out
    assert code == 3
    assert "Keine einzelne Standard-Aenderung" in out


# --- Schaerfere Modi ---------------------------------------------------------------------


def test_fail_on_new_edge_greift_unabhaengig_vom_takt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(
        tmp_path, [{"dags/pipeline.py": V1_TAEGLICH}, {"dags/pipeline.py": V2_TAEGLICH}]
    )
    code = main(["check", str(repo / "dags"), "--against", "v1", "--fail-on-new-edge"])
    assert code == 3
    assert "--fail-on-new-edge" in capsys.readouterr().out


def test_max_increase_deckelt_lambda_wachstum_ohne_neue_kante(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Gleiche Cross-Kante, laengerer Intra-Pfad im Kreis: Lambda 200 -> 300 s (+50 %).
    repo = repo_mit(tmp_path, [{"dags/ets.py": ETS_KURZ}, {"dags/ets.py": ETS_LANG}])
    argv = ["check", str(repo / "dags"), "--against", "v1", "--assume-duration", "100"]
    assert main([*argv, "--max-increase", "20"]) == 3
    out = capsys.readouterr().out
    assert "--max-increase" in out
    assert main([*argv, "--max-increase", "100"]) == 0
    capsys.readouterr()


# --- Worktree-Mechanik -------------------------------------------------------------------


def test_nutzer_repo_bleibt_unangetastet(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    status_vorher = git(repo, "status", "--porcelain")
    worktrees_vorher = git(repo, "worktree", "list")
    main(["check", str(repo / "dags"), "--against", "v1"])
    capsys.readouterr()
    assert git(repo, "status", "--porcelain") == status_vorher
    assert git(repo, "worktree", "list") == worktrees_vorher


def test_worktree_verschwindet_auch_bei_exception(tmp_path: Path) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}])
    with pytest.raises(RuntimeError, match="boom"), gate.worktree(repo, "v1") as wt:
        assert (wt / "dags/pipeline.py").exists()
        merker = wt
        raise RuntimeError("boom")
    assert not merker.exists()
    assert len(git(repo, "worktree", "list").splitlines()) == 1


# --- Exit-Code-Matrix --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fall", "erwartet"),
    [
        ("bestanden", 0),
        ("keine_dags", 0),
        ("ausgeloest", 3),
        ("pfad_fehlt", 1),
        ("ref_unaufloesbar", 1),
        ("kein_git_repo", 1),
        ("dag_id_unbekannt", 1),
    ],
)
def test_exit_code_matrix(
    fall: str, erwartet: int, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    if fall == "kein_git_repo":
        ort = tmp_path / "kein-repo"
        ort.mkdir()
        (ort / "pipeline.py").write_text(V1)
        argv = ["check", str(ort), "--against", "HEAD"]
    else:
        repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
        argv = {
            "bestanden": ["check", str(repo / "dags"), "--against", "v2"],
            "keine_dags": ["check", str(repo / "dags"), "--against", "v1"],
            "ausgeloest": ["check", str(repo / "dags"), "--against", "v1"],
            "pfad_fehlt": ["check", str(repo / "gibtsnicht"), "--against", "v1"],
            "ref_unaufloesbar": ["check", str(repo / "dags"), "--against", "gibtsnicht"],
            "dag_id_unbekannt": [
                "check",
                str(repo / "dags"),
                "--against",
                "v1",
                "--dag-id",
                "gibtsnicht",
            ],
        }[fall]
        if fall == "keine_dags":
            (repo / "dags/pipeline.py").write_text(KEIN_DAG)
            git(repo, "add", "-A")
            git(
                repo,
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-q",
                "--no-verify",
                "-m",
                "leer",
            )
            argv = ["check", str(repo / "dags"), "--against", "HEAD"]
    assert main(argv) == erwartet
    ausgabe = capsys.readouterr()
    if fall == "keine_dags":
        assert "Keine DAGs" in ausgabe.out
    if erwartet == 1:
        assert ausgabe.err != ""


def test_dag_id_filter_begrenzt_den_vergleich(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(
        tmp_path,
        [
            {"dags/pipeline.py": V1, "dags/ets.py": ETS_KURZ},
            {"dags/pipeline.py": V2, "dags/ets.py": ETS_KURZ},
        ],
    )
    code = main(["check", str(repo / "dags"), "--against", "v1", "--dag-id", "reconcile"])
    assert code == 0  # der Fail liegt im anderen DAG
    capsys.readouterr()


# --- Ausgabe-Formen ----------------------------------------------------------------------


def test_json_und_text_kommen_aus_einer_quelle(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    code = main(["check", str(repo / "dags"), "--against", "v1", "--json"])
    d = json.loads(capsys.readouterr().out)
    assert code == 3
    assert d["bestanden"] is False
    assert d["exit_code"] == 3
    assert d["modus"] == "struktur"
    dag = next(r for r in d["dags"] if r["dag_id"] == "load_data_wikiviews")
    assert dag["ausgeloest"] is True
    assert dag["neue_kanten"]
    assert dag["ausloeser_kante"]["zeile"] == V2_ZEILE
    assert dag["takt_s"] == 3600.0


def test_comment_file_schreibt_markdown(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    ziel = tmp_path / "kommentar.md"
    code = main(["check", str(repo / "dags"), "--against", "v1", "--comment-file", str(ziel)])
    assert code == 3
    inhalt = ziel.read_text()
    assert "ausgeloest" in inhalt
    assert f"pipeline.py:{V2_ZEILE}" in inhalt
    capsys.readouterr()


def test_kommentar_nennt_kreis_doppelt_und_modellgrenzen(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = repo_mit(tmp_path, [{"dags/pipeline.py": V1}, {"dags/pipeline.py": V2}])
    main(["check", str(repo / "dags"), "--against", "v1"])
    out = capsys.readouterr().out
    assert "kondensiert" in out.lower()
    assert "aufgeloest" in out.lower()
    assert "Untergrenze" in out
    assert "Task-Einheiten" in out  # Struktur-Modus sagt seine Einheit
