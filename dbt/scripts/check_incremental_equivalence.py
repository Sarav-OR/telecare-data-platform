"""Prove that incremental dbt runs give exactly the same marts as a full refresh.

    1. build the project on an initial silver snapshot            (e.g. data up to 15 Sep)
    2. append a later increment to silver and run dbt again       (incremental / MERGE path)
    3. full-refresh the same final data in a copy of the database (the reference)
    4. compare facts, dimensions and bridges row by row

    python dbt/scripts/check_incremental_equivalence.py \
        --initial ./work/bootstrap_run --final ./work/lakehouse --workdir ./work/inc_check

Exit code 1 if any table differs. Used in CI (Day 4) - this is the test that makes
late-arriving data handling trustworthy.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

DBT_DIR = Path(__file__).resolve().parents[1]
KEYS = {  # silver merge keys (same as the ingestion registry)
    "call_events": ["event_id"], "app_events": ["event_id"], "device_measurements": ["measurement_id"],
    "device_registry": ["device_serial", "snapshot_date"], "crm_insurers": ["insurer_id", "snapshot_date"],
    "crm_insurance_plans": ["plan_code", "snapshot_date"], "crm_patient_coverage": ["coverage_id", "snapshot_date"],
    "crm_partners": ["partner_id", "snapshot_date"], "ehr_patient": ["patient_id", "modified_at"],
    "ehr_patient_identifier": ["identifier_id", "modified_at"], "ehr_staff": ["staff_id", "modified_at"],
    "ehr_encounter": ["encounter_id", "modified_at"],
    "ehr_encounter_diagnosis": ["encounter_id", "seq_no", "modified_at"],
    "ehr_prescription": ["prescription_id", "modified_at"], "ehr_referral": ["referral_id", "modified_at"],
    "ehr_sick_note": ["sick_note_id", "modified_at"],
}
COMPARE = {
    "fact_encounter": "encounter_key, patient_sk, staff_sk, plan_sk, primary_diagnosis_key, disposition_key, "
                      "status, is_deleted, diagnosis_count, prescription_count, referral_count, tariff_amount_chf, "
                      "wait_seconds, age_at_encounter",
    "fact_contact": "contact_key, patient_sk, agent_staff_sk, contact_outcome_key, encounter_key, queue_seconds",
    "fact_diagnostic_measurement": "measurement_key, patient_sk, device_sk, partner_sk, alert_level, metric_value",
    "dim_patient": "patient_sk, valid_from, valid_to, canton_code",
    "bridge_encounter_diagnosis": "encounter_key, seq_no, diagnosis_key, weighting_factor",
}


def dbt(db: Path, *args: str) -> None:
    env = dict(os.environ, DBT_DUCKDB_PATH=str(db), DBT_TARGET="ci", DBT_PROFILES_DIR=str(DBT_DIR))
    subprocess.run(["dbt", *args, "-q"], cwd=DBT_DIR, env=env, check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--initial", required=True, help="lakehouse folder of the initial run")
    ap.add_argument("--final", required=True, help="lakehouse folder containing the complete data")
    ap.add_argument("--workdir", required=True)
    a = ap.parse_args()

    work = Path(a.workdir)
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    inc, ref, final_db = work / "incremental.duckdb", work / "reference.duckdb", work / "final_silver.duckdb"
    loader = DBT_DIR / "scripts" / "load_silver_to_duckdb.py"
    subprocess.run([sys.executable, loader, "--lakehouse", a.initial, "--db", inc], check=True, capture_output=True)
    subprocess.run([sys.executable, loader, "--lakehouse", a.final, "--db", final_db], check=True, capture_output=True)

    print("1/3 initial build")
    dbt(inc, "seed")
    dbt(inc, "run")

    print("2/3 append increment (rows not yet in silver, loaded 'later') and run incrementally")
    con = duckdb.connect(str(inc))
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{final_db}' AS final_db (READ_ONLY)")
    for table, keys in KEYS.items():
        on = " AND ".join(f"b.{k} IS NOT DISTINCT FROM f.{k}" for k in keys)
        con.execute(f"""INSERT INTO silver.{table}
                        SELECT f.* REPLACE (now()::timestamp + INTERVAL 1 DAY AS ingested_at)
                        FROM final_db.silver.{table} f
                        WHERE NOT EXISTS (SELECT 1 FROM silver.{table} b WHERE {on})""")
    con.close()
    dbt(inc, "run")

    print("3/3 full refresh of the same data as reference")
    shutil.copy(inc, ref)
    dbt(ref, "run", "--full-refresh")

    con = duckdb.connect(str(inc), read_only=True)
    con.execute(f"ATTACH '{ref}' AS ref (READ_ONLY)")
    failed = False
    for table, cols in COMPARE.items():
        a_sql, b_sql = f"SELECT {cols} FROM incremental.marts.{table}", f"SELECT {cols} FROM ref.marts.{table}"
        only_inc = con.execute(f"SELECT COUNT(*) FROM ({a_sql} EXCEPT {b_sql})").fetchone()[0]
        only_ref = con.execute(f"SELECT COUNT(*) FROM ({b_sql} EXCEPT {a_sql})").fetchone()[0]
        rows = con.execute(f"SELECT COUNT(*) FROM incremental.marts.{table}").fetchone()[0]
        status = "OK " if only_inc == only_ref == 0 else "DIFF"
        failed |= status == "DIFF"
        print(f"{status} {table:<30} rows={rows:>8}  only_incremental={only_inc}  only_full_refresh={only_ref}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
