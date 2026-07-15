"""Verifikation 3 (Spec 009): percentile_cont-Pfad (Postgres) == Python-Pfad (SQLite)
auf denselben Fixture-Zeilen. Der Container ist Wegwerf-Ware, nichts persistiert."""

from __future__ import annotations

import datetime as dt
import sys

sys.path.insert(0, "/mnt/data/projects/eigenlag")

import sqlalchemy as sa

from eigenlag.durations import from_metadata_db

PG_URL = "postgresql+psycopg2://postgres:wegwerf@127.0.0.1:15432/postgres"
SQLITE = sys.argv[1]  # Pfad fuer die SQLite-Vergleichs-DB

now = dt.datetime.now(dt.UTC)
fresh_dt = now - dt.timedelta(days=1)
stale_dt = now - dt.timedelta(days=400)

ROWS: list[tuple[str, str, str, float | None, str | None, dt.datetime]] = []
for d in [10.0, 20.0, 30.0, 40.0, 50.0]:
    ROWS.append(("etl", "extract", "success", d, "PythonOperator", fresh_dt))
for d in [1.0, 2.0, 3.0, 4.0, 5.0]:
    ROWS.append(("etl", "grp.load", "success", d, "PythonOperator", fresh_dt))
for _ in range(5):
    ROWS.append(("etl", "wait", "success", 60.0, "ExternalTaskSensor", fresh_dt))
ROWS.append(("etl", "rare", "success", 2.0, None, fresh_dt))
ROWS.append(("etl", "rare", "success", 4.0, None, fresh_dt))
ROWS.append(("etl", "extract", "failed", 999.0, "PythonOperator", fresh_dt))
ROWS.append(("etl", "extract", "success", None, "PythonOperator", fresh_dt))
ROWS.append(("etl", "extract", "success", 99999.0, "PythonOperator", stale_dt))
ROWS.append(("anderer", "extract", "success", 777.0, "PythonOperator", fresh_dt))

INSERT = (
    "INSERT INTO task_instance (dag_id, task_id, state, duration, operator, start_date)"
    " VALUES (:d, :t, :s, :dur, :op, :sd)"
)


def fill(url: str, ddl: str, to_sd: object) -> None:
    engine = sa.create_engine(url)
    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS task_instance"))
        conn.execute(sa.text(ddl))
        conn.execute(
            sa.text(INSERT),
            [
                {"d": d, "t": t, "s": s, "dur": dur, "op": op, "sd": to_sd(sd)}  # type: ignore[operator]
                for (d, t, s, dur, op, sd) in ROWS
            ],
        )
    engine.dispose()


fill(
    PG_URL,
    "CREATE TABLE task_instance (dag_id TEXT, task_id TEXT, state TEXT,"
    " duration DOUBLE PRECISION, operator TEXT, start_date TIMESTAMPTZ)",
    lambda sd: sd,
)
fill(
    f"sqlite:///{SQLITE}",
    "CREATE TABLE task_instance (dag_id TEXT, task_id TEXT, state TEXT,"
    " duration FLOAT, operator TEXT, start_date TIMESTAMP)",
    lambda sd: sd.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f"),
)

pg_stats, pg_warn = from_metadata_db(PG_URL, ["etl"], since_days=90)
lite_stats, lite_warn = from_metadata_db(f"sqlite:///{SQLITE}", ["etl"], since_days=90)

assert pg_warn == lite_warn == ()
assert set(pg_stats) == set(lite_stats), (set(pg_stats), set(lite_stats))
for task in sorted(pg_stats):
    pg, lite = pg_stats[task], lite_stats[task]
    print(
        f"{task:15s} pg:  n={pg.n} mean={pg.mean:g} p50={pg.p50:g} p95={pg.p95:g} op={pg.operator}"
    )
    print(
        f"{'':15s} lite:n={lite.n} mean={lite.mean:g} p50={lite.p50:g} p95={lite.p95:g}"
        f" op={lite.operator}"
    )
    assert (pg.n, pg.operator) == (lite.n, lite.operator)
    for attr in ("mean", "p50", "p95"):
        a, b = getattr(pg, attr), getattr(lite, attr)
        assert abs(a - b) < 1e-9, (task, attr, a, b)
print("\nPostgres (percentile_cont) == SQLite (Python-Aggregation): identisch.")
