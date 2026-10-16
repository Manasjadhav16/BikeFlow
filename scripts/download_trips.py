"""Download NYC Citi Bike monthly trip zips and convert them to Parquet, one month at a time.

Disk-frugal: CSV members are extracted and ingested one at a time, then deleted;
the zip is deleted as soon as the month is converted. Months whose partition has
a _BIKEFLOW_DONE marker are skipped, so re-running resumes where it stopped.

Usage:
    python scripts/download_trips.py --months 2026-07 2026-08 2026-09
    python scripts/download_trips.py --latest 3
"""
import argparse
import re
import shutil
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.common import (DATA_RAW, METRICS_DIR, TRIPS_DIR, check_disk, dir_size_bytes,  # noqa: E402
                        get_spark, write_json)
from src.ingest import EXPECTED_COLUMNS, ingest_csvs  # noqa: E402

BUCKET_URL = "https://s3.amazonaws.com/tripdata"
# NYC monthly files only (JC-* is Jersey City; YYYY-citibike-tripdata.zip are yearly archives).
MONTHLY_KEY = re.compile(r"^(\d{4})(\d{2})-citibike-tripdata(?:\.csv)?\.zip$")
# The new schema (ride_id, rideable_type, ...) starts in Feb 2021.
NEW_SCHEMA_FROM = "2021-02"
DONE_MARKER = "_BIKEFLOW_DONE"
RAW_SIZES_JSON = METRICS_DIR / "raw_sizes.json"


def list_monthly_keys() -> dict[str, dict]:
    """Return {'YYYY-MM': {'key': ..., 'size': ...}} from the S3 XML listing."""
    found, token = {}, None
    while True:
        params = {"list-type": "2"}
        if token:
            params["continuation-token"] = token
        r = requests.get(BUCKET_URL, params=params, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"s3": root.tag.split("}")[0].strip("{")} if root.tag.startswith("{") else {}
        q = (lambda t: f"s3:{t}") if ns else (lambda t: t)
        for c in root.findall(q("Contents"), ns):
            key = c.findtext(q("Key"), namespaces=ns)
            m = MONTHLY_KEY.match(key or "")
            if m:
                found[f"{m[1]}-{m[2]}"] = {"key": key, "size": int(c.findtext(q("Size"), namespaces=ns))}
        if root.findtext(q("IsTruncated"), namespaces=ns) == "true":
            token = root.findtext(q("NextContinuationToken"), namespaces=ns)
        else:
            return found


def partition_dir(ym: str) -> Path:
    y, m = ym.split("-")
    return TRIPS_DIR / f"year={int(y)}" / f"month={int(m)}"


def download(url: str, dest: Path, expected: int, retries: int = 8) -> None:
    """Stream url to dest, resuming a partial file with HTTP Range after dropped connections."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    for attempt in range(1, retries + 1):
        have = dest.stat().st_size if dest.exists() else 0
        if have == expected:
            break
        if have > expected:
            dest.unlink()
            have = 0
        if have:
            print(f"  resuming at {have / 1e6:.0f} MB (attempt {attempt})")
        try:
            headers = {"Range": f"bytes={have}-"} if have else {}
            with requests.get(url, stream=True, timeout=120, headers=headers) as r:
                r.raise_for_status()
                if have and r.status_code != 206:  # server ignored Range: start over
                    have = 0
                with open(dest, "ab" if have else "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        f.write(chunk)
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            print(f"  connection dropped ({type(e).__name__}); retrying in {5 * attempt}s")
            time.sleep(5 * attempt)
    got = dest.stat().st_size if dest.exists() else 0
    if got != expected:
        raise RuntimeError(f"{dest.name}: got {got} bytes, expected {expected}")
    print(f"  downloaded {got / 1e6:.0f} MB in {time.time() - t0:.0f}s")


def csv_members(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    return sorted(
        (i for i in zf.infolist()
         if i.filename.lower().endswith(".csv") and not i.is_dir()
         and "__MACOSX" not in i.filename and not Path(i.filename).name.startswith("._")),
        key=lambda i: i.filename,
    )


def check_header(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> None:
    with zf.open(info) as f:
        header = f.readline().decode("utf-8-sig").strip()
    cols = [c.strip().strip('"') for c in header.split(",")]
    if cols != EXPECTED_COLUMNS:
        raise ValueError(f"{info.filename} is not the new schema: {cols}")


def load_raw_sizes() -> dict:
    import json
    return json.loads(RAW_SIZES_JSON.read_text()) if RAW_SIZES_JSON.exists() else {}


def process_month(spark, ym: str, entry: dict) -> None:
    part = partition_dir(ym)
    if part.exists():
        print(f"  removing partial partition {part}")
        shutil.rmtree(part)
    work = DATA_RAW / ym
    zip_path = work / entry["key"]
    csv_dir = work / "csv"

    check_disk(f"{ym} download")
    download(f"{BUCKET_URL}/{entry['key']}", zip_path, entry["size"])

    members_log, csv_total = [], 0
    with zipfile.ZipFile(zip_path) as zf:
        members = csv_members(zf)
        if not members:
            raise RuntimeError(f"{zip_path.name} has no CSV members")
        check_header(zf, members[0])
        csv_dir.mkdir(parents=True, exist_ok=True)
        for i, info in enumerate(members, 1):
            check_disk(f"{ym} extract {i}/{len(members)}")
            check_header(zf, info)
            out = csv_dir / Path(info.filename).name  # basename only: no path traversal
            with zf.open(info) as src, open(out, "wb") as dst:
                shutil.copyfileobj(src, dst, 8 << 20)
            size = out.stat().st_size
            csv_total += size
            members_log.append({"name": info.filename, "csv_bytes": size})
            ingest_csvs(spark, [str(out)], int(ym[:4]), int(ym[5:]))
            out.unlink()
            print(f"  [{i}/{len(members)}] {out.name}: {size / 1e6:.0f} MB csv -> parquet, csv deleted")

    rows = spark.read.parquet(str(part)).count()
    sizes = load_raw_sizes()
    sizes[ym] = {
        "key": entry["key"],
        "zip_bytes": zip_path.stat().st_size,
        "csv_bytes": csv_total,
        "parquet_bytes": dir_size_bytes(part),
        "csv_members": members_log,
        "rows": rows,
        "converted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    write_json(RAW_SIZES_JSON, dict(sorted(sizes.items())))  # logged before the zip is removed
    shutil.rmtree(work)
    (part / DONE_MARKER).touch()
    s = sizes[ym]
    print(f"  {ym}: {rows:,} rows | zip {s['zip_bytes'] / 1e6:.0f} MB, csv {csv_total / 1e6:.0f} MB, "
          f"parquet {s['parquet_bytes'] / 1e6:.0f} MB | zip deleted")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--months", nargs="+", help="YYYY-MM months to fetch")
    g.add_argument("--latest", type=int, help="fetch the N most recent available months")
    p.add_argument("--list", action="store_true", help="only print what would be done")
    a = p.parse_args()

    available = list_monthly_keys()
    if a.latest:
        months = sorted(m for m in available if m >= NEW_SCHEMA_FROM)[-a.latest:]
    else:
        months = a.months
        bad = [m for m in months if not re.fullmatch(r"\d{4}-\d{2}", m)]
        missing = [m for m in months if m not in available]
        old = [m for m in months if m < NEW_SCHEMA_FROM]
        if bad or missing or old:
            sys.exit(f"invalid months: format={bad} not_in_bucket={missing} old_schema={old}")

    todo = [m for m in months if not (partition_dir(m) / DONE_MARKER).exists()]
    for m in months:
        state = "todo" if m in todo else "done, skipping"
        print(f"{m}: {available[m]['key']} ({available[m]['size'] / 1e6:.0f} MB) -> {state}")
    if a.list or not todo:
        return

    spark = get_spark("bikeflow-download")
    try:
        for m in todo:
            print(f"== {m}")
            process_month(spark, m, available[m])
    finally:
        spark.stop()
    check_disk("download finished")


if __name__ == "__main__":
    main()
