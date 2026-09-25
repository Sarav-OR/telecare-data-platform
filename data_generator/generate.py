"""Generate the TeleCare CH synthetic data for all five source systems.

    python data_generator/generate.py --out ./work/telecare --days 90 --patients 20000

Output layout
    vendor-drop/telephony/call_events/YYYY/MM/DD/HH/*.jsonl      S1 telephony (hourly)
    vendor-drop/app/app_events/YYYY/MM/DD/HH/*.jsonl              S2 patient app (hourly)
    ehr/<table>.csv                                               S3 EHR row versions -> Azure SQL
    vendor-drop/crm/<feed>/YYYY-MM-DD/*.csv                       S4 CRM weekly snapshots (';', dd.mm.yyyy)
    api/partners/YYYY-MM-DD/page-NNNN.json                        S4 partner directory REST payloads
    vendor-drop/devices/measurements/YYYY/MM/DD/HH/*.jsonl        S5 pharmacy device telemetry
    vendor-drop/devices/device_registry/YYYY-MM-DD/*.csv          S5 device registry (weekly)
    manifest.json                                                 row counts + injected issue counts
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import date
from pathlib import Path

from telecare_gen.generator import TeleCareGenerator


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="./work/telecare")
    ap.add_argument("--start-date", default="2026-06-25")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--patients", type=int, default=20000)
    ap.add_argument("--contacts-per-day", type=int, default=2200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--issue-scale", type=float, default=1.0, help="0 = clean data, 1 = default issue rates")
    ap.add_argument("--api-base-url", default="https://<storage-account>.z1.web.core.windows.net/api",
                    help="base URL used in the partner API 'next' links (static website of the storage account)")
    args = ap.parse_args()

    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    t = time.time()
    g = TeleCareGenerator(out, date.fromisoformat(args.start_date), args.days, args.patients,
                          args.contacts_per_day, args.seed, args.issue_scale, args.api_base_url)
    g.run()
    summary = {"parameters": vars(args), "counts": g.summary(), "seconds": round(time.time() - t, 1)}
    (out / "manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary["counts"], indent=2))
    print(f"done in {summary['seconds']} s -> {out.resolve()}")


if __name__ == "__main__":
    main()
