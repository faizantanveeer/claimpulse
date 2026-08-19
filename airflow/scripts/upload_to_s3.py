"""
ClaimPulse S3 uploader.

Uploads locally generated CSV batches into the streaming-ready S3 layout:

  raw/source=<batch|streaming>/table=<name>/year=YYYY/month=MM/day=DD/
      <table>_<UTC timestamp>_<short hash>.csv

  _manifests/source=.../table=.../year=/month=/day=/
      manifest_<timestamp>.json   -- row counts + md5 checksums for every
                                      file in the batch, for fault-tolerant
                                      reconciliation against what Snowflake
                                      actually ingested

  _quarantine/table=.../year=/month=/day=/
      files that fail structural validation before ever reaching raw/

Design notes:
  - Retries use botocore's adaptive mode (exponential backoff + jitter),
    not a hand-rolled retry loop -- this is what AWS itself recommends
    and it correctly backs off on throttling, not just outright failures.
  - Every upload is idempotent by construction: the timestamp+hash in the
    filename means re-running this script never overwrites a prior file,
    so a crashed mid-run script is always safe to re-run.
  - Credentials are never read from arguments or hardcoded -- boto3 picks
    them up from environment variables / ~/.aws/credentials automatically.
"""

import csv
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("claimpulse-uploader")

BUCKET_NAME = "claimspulse-claim-processing"
OUTPUT_DIR = Path(os.environ.get("CLAIMPULSE_DATA_DIR", Path(__file__).parent / "output"))
EXPECTED_COLUMNS = {
    "carriers": {"carrier_id", "carrier_name", "tpa_flag"},
    "providers": {"provider_id", "provider_name", "specialty", "state"},
    "claims": {"claim_id", "provider_id", "carrier_id", "date_of_loss",
               "claim_open_date", "claim_status", "state"},
    "carrier_letters": {"letter_id", "claim_id", "carrier_id", "letter_type",
                         "received_date", "response_due_date", "classification_status"},
    "training_records": {"training_id", "provider_id", "carrier_id",
                          "assigned_date", "completed_date", "status"},
}

# Adaptive retry mode: exponential backoff with jitter, and it specifically
# understands S3 throttling responses rather than treating every failure
# the same way. max_attempts includes the first try.
s3_client = boto3.client(
    "s3",
    config=Config(retries={"max_attempts": 6, "mode": "adaptive"}),
)


def md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def count_rows(path: Path) -> int:
    with open(path, newline="") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def validate_columns(path: Path, table_name: str) -> bool:
    expected = EXPECTED_COLUMNS.get(table_name)
    if expected is None:
        return True
    with open(path, newline="") as f:
        actual = set(next(csv.reader(f)))
    if actual != expected:
        log.warning(f"  schema mismatch for {path.name}: "
                    f"missing={expected - actual}, unexpected={actual - expected}")
        return False
    return True


def build_key(source: str, table_name: str, dt: datetime, filename: str, prefix: str = "raw") -> str:
    return (f"{prefix}/source={source}/table={table_name}/"
            f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/{filename}")


def upload_file_with_retry(local_path: Path, key: str):
    log.info(f"  uploading -> s3://{BUCKET_NAME}/{key}")
    # boto3's upload_file automatically does multipart upload for large
    # files (default threshold 8MB) and resumes/retries chunks internally --
    # no extra config needed at current data volumes, but this is the same
    # call that scales to multi-GB files without any code change.
    s3_client.upload_file(str(local_path), BUCKET_NAME, key)


def upload_manifest(source: str, table_name: str, dt: datetime, manifest: dict):
    ts = dt.strftime("%Y%m%dT%H%M%SZ")
    key = build_key(source, table_name, dt, f"manifest_{ts}.json", prefix="_manifests")
    body = json.dumps(manifest, indent=2).encode("utf-8")
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
    log.info(f"  manifest -> s3://{BUCKET_NAME}/{key}")


def quarantine_file(local_path: Path, table_name: str, dt: datetime):
    key = build_key("batch", table_name, dt, local_path.name, prefix="_quarantine")
    upload_file_with_retry(local_path, key)
    log.warning(f"  quarantined: s3://{BUCKET_NAME}/{key}")


def process_table(table_name: str, local_dir: Path, source: str = "batch"):
    log.info(f"Processing table: {table_name}")
    now = datetime.now(timezone.utc)
    manifest_entries = []

    for csv_file in sorted(local_dir.glob("*.csv")):
        if not validate_columns(csv_file, table_name):
            quarantine_file(csv_file, table_name, now)
            continue

        row_count = count_rows(csv_file)
        checksum = md5_of_file(csv_file)
        ts = now.strftime("%Y%m%dT%H%M%SZ")
        short_hash = uuid.uuid4().hex[:6]
        s3_filename = f"{table_name}_{ts}_{short_hash}.csv"
        key = build_key(source, table_name, now, s3_filename)

        upload_file_with_retry(csv_file, key)

        manifest_entries.append({
            "file": s3_filename,
            "s3_key": key,
            "row_count": row_count,
            "md5": checksum,
            "uploaded_at": now.isoformat(),
        })

    if manifest_entries:
        manifest = {
            "table": table_name,
            "source": source,
            "batch_uploaded_at": now.isoformat(),
            "file_count": len(manifest_entries),
            "total_rows": sum(e["row_count"] for e in manifest_entries),
            "files": manifest_entries,
        }
        upload_manifest(source, table_name, now, manifest)
        log.info(f"  done: {manifest['file_count']} files, {manifest['total_rows']} rows")
    else:
        log.info("  no valid files found to upload")


def process_reference_table(table_name: str, local_dir: Path, source: str = "batch"):
    """Reference tables (carriers, providers) are generated once and reused --
    the CSV here is unpartitioned and typically unchanged run over run. Upload
    it under a fixed, non-timestamped key instead of process_table's unique
    timestamp+hash filename: COPY INTO's load history dedups by file path, so
    a stable key lets an unchanged file get correctly skipped on every
    subsequent run instead of being re-ingested as "new" data every time.
    """
    log.info(f"Processing table: {table_name} (reference)")
    now = datetime.now(timezone.utc)

    csv_files = sorted(local_dir.glob("*.csv"))
    if not csv_files:
        log.info("  no valid files found to upload")
        return
    csv_file = csv_files[0]

    if not validate_columns(csv_file, table_name):
        quarantine_file(csv_file, table_name, now)
        return

    row_count = count_rows(csv_file)
    checksum = md5_of_file(csv_file)
    key = f"raw/source={source}/table={table_name}/{table_name}.csv"

    upload_file_with_retry(csv_file, key)

    manifest = {
        "table": table_name,
        "source": source,
        "batch_uploaded_at": now.isoformat(),
        "file_count": 1,
        "total_rows": row_count,
        "files": [{
            "file": csv_file.name,
            "s3_key": key,
            "row_count": row_count,
            "md5": checksum,
            "uploaded_at": now.isoformat(),
        }],
    }
    upload_manifest(source, table_name, now, manifest)
    log.info(f"  done: 1 files, {row_count} rows (fixed key -- COPY INTO skips if unchanged)")


def main():
    if not OUTPUT_DIR.exists():
        log.error(f"No output directory found at {OUTPUT_DIR}. Run generate_data.py first.")
        sys.exit(1)

    for table_dir in sorted(OUTPUT_DIR.iterdir()):
        if not table_dir.is_dir():
            continue
        table_name = table_dir.name
        dt_dirs = sorted(table_dir.glob("dt=*"))
        if dt_dirs:
            # event data: each table dir contains dt=YYYY-MM-DD subfolders
            for dt_dir in dt_dirs:
                process_table(table_name, dt_dir)
        else:
            # reference data: CSVs sit directly in the table dir, no date partition
            process_reference_table(table_name, table_dir)


if __name__ == "__main__":
    main()