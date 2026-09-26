"""Run a T-SQL script (with GO batch separators) against Azure SQL using your Entra identity.

The portal Query editor does not understand `GO`; this does, with the same `az login`
token and retry-on-resume logic as the EHR loader.

    python data_generator/run_sql_script.py --server sql-telecare-dev-chn.database.windows.net \
        --database sqldb-telecare-ehr --file sql/ctl/03_control_tables.sql
"""

from __future__ import annotations

import argparse
from pathlib import Path

from load_ehr_to_sql import connect, run_ddl


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--file", required=True, action="append", help="script to run (repeatable, runs in order)")
    ap.add_argument("--auth", choices=["token", "sql"], default="token")
    ap.add_argument("--user")
    a = ap.parse_args()
    conn = connect(a.server, a.database, a.auth, a.user)
    for f in a.file:
        run_ddl(conn, Path(f))
        print(f"ok  {f}")
    conn.close()


if __name__ == "__main__":
    main()
