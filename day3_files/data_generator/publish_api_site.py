"""Publish the partner-directory API pages as a static website (the "REST API" source).

The CRM vendor exposes its partner directory as a paginated REST API. We simulate it with
the storage account's static website: every page is a JSON document with an absolute `next`
link - exactly the pagination rule ADF's REST connector follows ("AbsoluteUrl" = $.next).

    python data_generator/publish_api_site.py --account sttelecaredevchn \
        --source ./work/telecare/api --base-url https://sttelecaredevchn.z1.web.core.windows.net/api

`--base-url` is the static website endpoint (Portal: storage account -> Static website ->
Primary endpoint) + "/api". The generator wrote placeholder `next` links; they are rewritten here.
Requirements: azure-identity, azure-storage-blob (installed with azure-storage-file-datalake).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account", required=True)
    ap.add_argument("--source", required=True, help="generated api folder (contains partners/<date>/page-*.json)")
    ap.add_argument("--base-url", required=True, help="https://<account>.<zone>.web.core.windows.net/api")
    a = ap.parse_args()

    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient, ContentSettings

    base = a.base_url.rstrip("/")
    web = BlobServiceClient(f"https://{a.account}.blob.core.windows.net",
                            credential=DefaultAzureCredential()).get_container_client("$web")
    source = Path(a.source)
    n = 0
    for page in sorted(source.rglob("page-*.json")):
        rel = page.relative_to(source).as_posix()                    # partners/<date>/page-0001.json
        body = json.loads(page.read_text(encoding="utf-8"))
        if body.get("next"):
            body["next"] = f"{base}/{rel.rsplit('/', 1)[0]}/{body['next'].rsplit('/', 1)[1]}"
        web.upload_blob(f"api/{rel}", json.dumps(body, ensure_ascii=False), overwrite=True,
                        content_settings=ContentSettings(content_type="application/json"))
        n += 1
    print(f"published {n} pages -> {base}/partners/<date>/page-0001.json")


if __name__ == "__main__":
    main()
