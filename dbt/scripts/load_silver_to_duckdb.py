"""Load the locally produced silver + ops Parquet into DuckDB so dbt can build and test
the full project without cloud access (used by CI and for local development).

    python dbt/scripts/load_silver_to_duckdb.py --lakehouse ./work/lakehouse --db ./work/telecare_ci.duckdb

Spark writes timestamps as UTC instants (TIMESTAMPTZ in DuckDB). Databricks TIMESTAMP
columns are also UTC instants, so they are stored as naive UTC TIMESTAMPs here - the dbt
SQL then behaves identically on both engines.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lakehouse", required=True, help="output folder of run_local_pipeline.py")
    ap.add_argument("--db", required=True)
    args = ap.parse_args()

    root, db = Path(args.lakehouse), Path(args.db)
    db.parent.mkdir(parents=True, exist_ok=True)
    if db.exists():
        db.unlink()
    con = duckdb.connect(str(db))
    con.execute("SET TimeZone = 'UTC'")
    for schema in ("silver", "ops"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for folder in sorted(p for p in (root / schema).iterdir() if p.is_dir()):
            src = f"read_parquet('{folder.as_posix()}/*.parquet')"
            cols = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
            select = ", ".join(
                f'"{c}" AT TIME ZONE \'UTC\' AS "{c}"' if t == "TIMESTAMP WITH TIME ZONE" else f'"{c}"'
                for c, t, *_ in cols)
            con.execute(f"CREATE TABLE {schema}.{folder.name} AS SELECT {select} FROM {src}")
            n = con.execute(f"SELECT COUNT(*) FROM {schema}.{folder.name}").fetchone()[0]
            print(f"{schema}.{folder.name:<26} {n:>9,}")
    con.close()


if __name__ == "__main__":
    main()
