"""Build the landing zone exactly as the ADF pipelines would write it - no Spark, no Java.

    python data_generator/build_landing.py --generated ./work/telecare --out ./work/bootstrap/landing --until 2026-09-15

What ADF does, simulated here:
    vendor-drop/*            -> landing/*                              binary copy, same folders
    api/partners/<d>/page-*  -> landing/crm/partners/<d>/partners.json  REST copy, one JSON object per line
    ehr/<table>.csv          -> landing/ehr/<table>/load_date=<d>/part-00000.parquet
                                daily watermark extract: latest version of each key changed that day

Used for the one-off Day 2 bootstrap (from Day 3 on, ADF writes the landing zone itself) and by
the local Spark runner. Requirements: pip install pyarrow
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# EHR extract schema as ADF writes it from Azure SQL. Timestamps are the source's local wall-clock
# time (no offset); they are stored as UTC-typed values and converted to real UTC in silver.
TS, D, S, I, B = pa.timestamp("us", tz="UTC"), pa.date32(), pa.string(), pa.int32(), pa.bool_()
DEC = pa.decimal128(8, 2)
_AUDIT = [("is_deleted", B), ("created_at", TS), ("modified_at", TS)]
EHR = {
    "patient": (["patient_id"], [("patient_id", S), ("date_of_birth", D), ("sex", S), ("canton", S),
                                 ("postal_code", S), ("preferred_language", S)] + _AUDIT),
    "patient_identifier": (["identifier_id"], [("identifier_id", S), ("patient_id", S), ("identifier_type", S),
                                               ("identifier_value", S), ("verified_at", TS)] + _AUDIT),
    "staff": (["staff_id"], [("staff_id", S), ("staff_login", S), ("role", S), ("specialty", S), ("team", S),
                             ("languages", S), ("employment_pct", I), ("hired_at", TS), ("left_at", TS)] + _AUDIT),
    "encounter": (["encounter_id"], [("encounter_id", S), ("patient_id", S), ("staff_id", S), ("channel", S),
                                     ("service_line", S), ("contact_system", S), ("contact_ref", S),
                                     ("started_at", TS), ("ended_at", TS), ("triage_level", I),
                                     ("disposition_code", S), ("plan_code", S), ("tariff_amount_chf", DEC),
                                     ("status", S)] + _AUDIT),
    "encounter_diagnosis": (["encounter_id", "seq_no"], [("encounter_id", S), ("seq_no", I), ("icd10_code", S),
                                                         ("diagnosis_role", S)] + _AUDIT),
    "prescription": (["prescription_id"], [("prescription_id", S), ("encounter_id", S), ("atc_code", S),
                                           ("quantity", I), ("pharmacy_partner_id", S), ("issued_at", TS)] + _AUDIT),
    "referral": (["referral_id"], [("referral_id", S), ("encounter_id", S), ("partner_id", S),
                                   ("referral_type", S), ("urgency", S), ("issued_at", TS)] + _AUDIT),
    "sick_note": (["sick_note_id"], [("sick_note_id", S), ("encounter_id", S), ("incapacity_pct", I), ("days", I),
                                     ("valid_from", D), ("issued_at", TS)] + _AUDIT),
}
_DATE_IN_PATH = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/\d{2}/|/(\d{4}-\d{2}-\d{2})/")


def _path_date(rel: str) -> str | None:
    m = _DATE_IN_PATH.search("/" + rel)
    if not m:
        return None
    return m.group(4) or f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def _convert(value: str, typ: pa.DataType):
    if value == "":
        return None
    if typ == TS:
        return datetime.fromisoformat(value)
    if typ == D:
        return date.fromisoformat(value)
    if typ == I:
        return int(value)
    if typ == B:
        return value == "1"
    if typ == DEC:
        return Decimal(value)
    return value


def build(generated: Path, landing: Path, window_start: str, until: str | None = None) -> int:
    """Write the landing zone; returns the number of files written."""
    files = 0
    src = generated / "vendor-drop"
    for f in src.rglob("*"):
        if f.is_file():
            rel = f.relative_to(src).as_posix()
            d = _path_date(rel)
            if until is None or d is None or d <= until:
                (landing / rel).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, landing / rel)
                files += 1

    for snap in sorted((generated / "api" / "partners").iterdir()):
        if until is not None and snap.name > until:
            continue
        items = []
        for page in sorted(snap.glob("page-*.json")):
            body = json.loads(page.read_text(encoding="utf-8"))
            items += [dict(i, snapshot_date=body["snapshot_date"]) for i in body["data"]]
        target = landing / "crm" / "partners" / snap.name
        target.mkdir(parents=True, exist_ok=True)
        (target / "partners.json").write_text("\n".join(json.dumps(i, ensure_ascii=False) for i in items) + "\n",
                                              encoding="utf-8")
        files += 1

    for table, (keys, fields) in EHR.items():
        schema = pa.schema(fields)
        latest: dict[tuple, dict] = {}
        with (generated / "ehr" / f"{table}.csv").open(encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                day = max(row["modified_at"][:10], window_start)       # pre-window history -> first extract
                if until is not None and day > until:
                    continue
                k = (day, *(row[c] for c in keys))
                if k not in latest or row["modified_at"] >= latest[k]["modified_at"]:
                    latest[k] = row
        by_day: dict[str, list[dict]] = defaultdict(list)
        for (day, *_), row in latest.items():
            by_day[day].append(row)
        shutil.rmtree(landing / "ehr" / table, ignore_errors=True)
        for day, rows in sorted(by_day.items()):
            cols = {name: [_convert(r[name], typ) for r in rows] for name, typ in fields}
            folder = landing / "ehr" / table / f"load_date={day}"
            folder.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table(cols, schema=schema), folder / "part-00000.snappy.parquet", compression="snappy")
            files += 1
    return files


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--generated", required=True, help="output folder of generate.py")
    ap.add_argument("--out", required=True, help="landing folder to create")
    ap.add_argument("--window-start", default="2026-06-25")
    ap.add_argument("--until", default=None, help="only land data up to this date (YYYY-MM-DD, inclusive)")
    a = ap.parse_args()
    out = Path(a.out)
    if out.exists():
        shutil.rmtree(out)
    n = build(Path(a.generated), out, a.window_start, a.until)
    print(f"landing zone ready: {n} files -> {out.resolve()}")


if __name__ == "__main__":
    main()
