"""Output writers. Each source system gets the format its real counterpart would use."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ZURICH = ZoneInfo("Europe/Zurich")


def iso_z(ts: datetime, millis: bool = True) -> str:
    s = ts.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return (s[:-3] if millis else s[:-7]) + "Z"


def ehr_local(ts: datetime | None) -> str:
    """The EHR stores local Swiss time without offset (a classic source-system quirk)."""
    return "" if ts is None else ts.astimezone(ZURICH).strftime("%Y-%m-%d %H:%M:%S")


def swiss_date(d: date | None) -> str:
    """CRM exports use the Swiss date format dd.mm.yyyy."""
    return "" if d is None else d.strftime("%d.%m.%Y")


class HourlyJsonlWriter:
    """Buffers events and appends them to one JSON Lines file per hour.

    Files are keyed by the timestamp the *source system* ships them at, so late or
    re-sent events naturally end up in later files, exactly like a real feed.
    """

    def __init__(self, root: Path, subpath: str, prefix: str):
        self.root, self.subpath, self.prefix = root, subpath, prefix
        self.buffer: dict[datetime, list[str]] = defaultdict(list)
        self.files: set[Path] = set()
        self.rows = 0

    def add(self, ship_ts: datetime, record: dict | str) -> None:
        hour = ship_ts.astimezone(ZoneInfo("UTC")).replace(minute=0, second=0, microsecond=0)
        line = record if isinstance(record, str) else json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        self.buffer[hour].append(line)
        self.rows += 1

    def flush(self) -> None:
        for hour, lines in self.buffer.items():
            folder = self.root / self.subpath / f"{hour:%Y}" / f"{hour:%m}" / f"{hour:%d}" / f"{hour:%H}"
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"{self.prefix}_{hour:%Y%m%d%H}.jsonl"
            with path.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
            self.files.add(path)
        self.buffer.clear()


class CsvTableWriter:
    """Append-only CSV of row *versions* for one EHR table (loaded into Azure SQL later)."""

    def __init__(self, path: Path, columns: list[str]):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path, self.columns = path, columns
        self.fh = path.open("w", newline="", encoding="utf-8")
        self.w = csv.DictWriter(self.fh, fieldnames=columns)
        self.w.writeheader()
        self.rows = 0

    def write(self, row: dict) -> None:
        self.w.writerow({c: row.get(c, "") for c in self.columns})
        self.rows += 1

    def close(self) -> None:
        self.fh.close()


def write_snapshot_csv(path: Path, rows: list[dict], columns: list[str], delimiter: str = ",") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, delimiter=delimiter)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})


def write_api_pages(folder: Path, items: list[dict], page_size: int, snapshot: date, base_url: str) -> int:
    """Paginated REST payloads with an absolute `next` link (ADF REST pagination rule)."""
    folder.mkdir(parents=True, exist_ok=True)
    pages = max(1, -(-len(items) // page_size))
    for p in range(1, pages + 1):
        chunk = items[(p - 1) * page_size: p * page_size]
        body = {
            "snapshot_date": snapshot.isoformat(),
            "page": p, "page_size": page_size, "total_items": len(items), "total_pages": pages,
            "data": chunk,
            "next": f"{base_url}/page-{p + 1:04d}.json" if p < pages else None,
        }
        (folder / f"page-{p:04d}.json").write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
    return pages
