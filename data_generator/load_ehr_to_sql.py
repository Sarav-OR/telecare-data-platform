"""Load the simulated EHR into Azure SQL Database, day by day.

The generator writes every *version* of every EHR row (an encounter is opened,
then closed, then maybe re-coded days later). This loader replays those versions
up to an "as of" moment, so the database looks exactly like a live clinical
system would at that time. Running it again with a later --as-of applies the
next day's inserts, updates and soft deletes, which is what ADF's watermark-based
incremental copy then picks up.

    # initial load: everything up to 15 Sep 2026 (local Swiss time)
    python load_ehr_to_sql.py --server sql-telecare-dev-chn.database.windows.net \
        --database sqldb-telecare-ehr --data ../work/telecare/ehr --as-of "2026-09-15 23:59:59"

    # next "day" in the source system
    python load_ehr_to_sql.py ... --as-of "2026-09-16 23:59:59"

Authentication
    --auth token (default): uses your `az login` session (Microsoft Entra), no password
    --auth sql            : SQL login, password read from the SQL_PASSWORD environment variable

Requirements: pip install pyodbc azure-identity ; Microsoft ODBC Driver 18 for SQL Server.
"""

from __future__ import annotations

import argparse
import csv
import os
import struct
import sys
import time
from pathlib import Path

TABLES = {                      # table -> primary key columns (load order does not matter: no FKs)
    "patient": ["patient_id"],
    "patient_identifier": ["identifier_id"],
    "staff": ["staff_id"],
    "encounter": ["encounter_id"],
    "encounter_diagnosis": ["encounter_id", "seq_no"],
    "prescription": ["prescription_id"],
    "referral": ["referral_id"],
    "sick_note": ["sick_note_id"],
}
BATCH = 5000


def latest_versions(path: Path, pk: list[str], since: str | None, until: str) -> tuple[list[str], list[list]]:
    """Latest version per primary key with since < modified_at <= until.

    Timestamps are 'YYYY-MM-DD HH:MM:SS' strings, so string comparison is chronological.
    Empty strings become NULL.
    """
    latest: dict[tuple, dict] = {}
    with path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = reader.fieldnames or []
        for row in reader:
            m = row["modified_at"]
            if m > until or (since is not None and m <= since):
                continue
            key = tuple(row[c] for c in pk)
            cur = latest.get(key)
            if cur is None or m >= cur["modified_at"]:
                latest[key] = row
    rows = [[(v if v != "" else None) for v in (r[c] for c in columns)] for r in latest.values()]
    return columns, rows


def connect(server: str, database: str, auth: str, user: str | None):
    import pyodbc

    base = (f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},1433;Database={database};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;")
    if auth == "sql":
        pwd = os.environ.get("SQL_PASSWORD")
        if not user or not pwd:
            sys.exit("--auth sql needs --user and the SQL_PASSWORD environment variable")
        return pyodbc.connect(base + f"Uid={user};Pwd={pwd};", autocommit=False)
    from azure.identity import DefaultAzureCredential

    token = DefaultAzureCredential().get_token("https://database.windows.net/.default").token.encode("utf-16-le")
    attrs = {1256: struct.pack(f"<I{len(token)}s", len(token), token)}   # SQL_COPT_SS_ACCESS_TOKEN
    return pyodbc.connect(base, attrs_before=attrs, autocommit=False)


def run_ddl(conn, ddl_file: Path) -> None:
    cur = conn.cursor()
    for batch in ddl_file.read_text(encoding="utf-8").split("\nGO"):
        if batch.strip():
            cur.execute(batch)
    conn.commit()


def upsert(conn, table: str, pk: list[str], columns: list[str], rows: list[list]) -> None:
    """Stage rows in a temp table, then MERGE: newer versions update, new keys insert."""
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.execute(f"IF OBJECT_ID('tempdb..#stg') IS NOT NULL DROP TABLE #stg; "
                f"SELECT TOP 0 * INTO #stg FROM ehr.{table};")
    placeholders = ",".join("?" * len(columns))
    col_list = ",".join(f"[{c}]" for c in columns)
    for i in range(0, len(rows), BATCH):
        cur.executemany(f"INSERT INTO #stg ({col_list}) VALUES ({placeholders})", rows[i:i + BATCH])
    on = " AND ".join(f"t.[{c}] = s.[{c}]" for c in pk)
    updates = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c not in pk)
    cur.execute(f"""
        MERGE ehr.{table} AS t
        USING #stg AS s ON {on}
        WHEN MATCHED AND s.modified_at > t.modified_at THEN UPDATE SET {updates}
        WHEN NOT MATCHED BY TARGET THEN INSERT ({col_list}) VALUES ({", ".join(f"s.[{c}]" for c in columns)});
    """)
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--data", required=True, help="folder with the generated ehr/*.csv files")
    ap.add_argument("--as-of", required=True, help="local Swiss time, e.g. '2026-09-15 23:59:59'")
    ap.add_argument("--auth", choices=["token", "sql"], default="token")
    ap.add_argument("--user", help="SQL login for --auth sql")
    ap.add_argument("--dry-run", action="store_true", help="only print what would be loaded")
    args = ap.parse_args()

    data = Path(args.data)
    ddl = Path(__file__).resolve().parents[1] / "sql" / "ehr" / "01_create_ehr_schema.sql"
    conn = None if args.dry_run else connect(args.server, args.database, args.auth, args.user)
    if conn:
        run_ddl(conn, ddl)

    for table, pk in TABLES.items():
        t = time.time()
        since = None
        if conn:
            sql = f"SELECT CONVERT(varchar(19), MAX(modified_at), 120) FROM ehr.{table}"
            since = conn.cursor().execute(sql).fetchone()[0]
        columns, rows = latest_versions(data / f"{table}.csv", pk, since, args.as_of)
        if conn and rows:
            upsert(conn, table, pk, columns, rows)
        print(f"{table:<22} since={since or '-':<19}  rows={len(rows):>7}  {time.time() - t:5.1f}s")
    if conn:
        conn.close()


if __name__ == "__main__":
    main()
