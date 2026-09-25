"""Upload a local folder to an ADLS Gen2 container (keeps the folder structure).

Uses your Microsoft Entra identity (`az login`) - no storage keys. You need the
"Storage Blob Data Contributor" role on the storage account.

    # the source systems' deliveries (ADF copies from here from Day 3 on)
    python publish_to_adls.py --account sttelecaredevchn --container vendor-drop --source ../work/telecare/vendor-drop

    # Day 2 bootstrap: landing zone as ADF would have produced it up to 15 Sep
    python publish_to_adls.py --account sttelecaredevchn --container landing --source ../work/bootstrap/landing

Files that already exist with the same size are skipped, so the command is safe to re-run.
Requirements: pip install azure-identity azure-storage-file-datalake
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account", required=True, help="storage account name, e.g. sttelecaredevchn")
    ap.add_argument("--container", required=True)
    ap.add_argument("--source", required=True, help="local folder to upload")
    ap.add_argument("--prefix", default="", help="optional folder inside the container")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    from azure.core.exceptions import ResourceNotFoundError
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    source = Path(args.source)
    # skip Spark/Hadoop side files (_SUCCESS, .crc): not data, and noise for Auto Loader
    files = [f for f in source.rglob("*") if f.is_file() and not f.name.startswith(("_", "."))]
    if not files:
        sys.exit(f"no files under {source}")
    service = DataLakeServiceClient(f"https://{args.account}.dfs.core.windows.net",
                                    credential=DefaultAzureCredential())
    fs = service.get_file_system_client(args.container)

    def upload(f: Path) -> str:
        rel = f.relative_to(source).as_posix()
        remote = f"{args.prefix.strip('/')}/{rel}" if args.prefix else rel
        client = fs.get_file_client(remote)
        if not args.overwrite:
            try:
                if client.get_file_properties().size == f.stat().st_size:
                    return "skipped"
            except ResourceNotFoundError:
                pass
        with f.open("rb") as fh:
            client.upload_data(fh, overwrite=True)
        return "uploaded"

    t, done, counts = time.time(), 0, {"uploaded": 0, "skipped": 0, "failed": 0}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(upload, f): f for f in files}
        for fut in as_completed(futures):
            try:
                counts[fut.result()] += 1
            except Exception as exc:  # noqa: BLE001  keep going, report at the end
                counts["failed"] += 1
                print(f"FAILED {futures[fut]}: {exc}", file=sys.stderr)
            done += 1
            if done % 500 == 0 or done == len(files):
                print(f"{done}/{len(files)} files  {counts}  {time.time() - t:.0f}s")
    if counts["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
