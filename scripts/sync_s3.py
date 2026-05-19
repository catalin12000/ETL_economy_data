"""
Sync pipeline outputs to S3.

Reads each pipeline's state.json to find the last downloaded raw file(s)
and the last deliverable, then uploads them to S3 under the agreed path template:

  raw_data/{cy|gr}/{source}/{YYYYMMDD}/{pipeline_id}_{timestamp}.ext
  transformed_data/{cy|gr}/{source}/{YYYYMMDD}/deliverable/{pipeline_id}_{timestamp}.csv

Usage:
  python scripts/sync_s3.py                  # sync all pipelines
  python scripts/sync_s3.py gdp_greece       # sync one pipeline
  python scripts/sync_s3.py --dry-run        # print keys without uploading
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from tqdm import tqdm

from etl.core.s3_upload import upload_pipeline_files, _PIPELINE_META
from etl.core.state import load_state
from etl.core.runner import list_pipelines


_RAW_PATH_KEYS = (
    "last_download_path",
    "last_download_path_receipts",
    "last_download_path_travellers",
    "last_download_path_turnover",
    "last_download_path_volume",
    "last_download_path_totals_2025",
    "last_download_path_totals_2026",
    "last_download_path_foreigners_2025",
    "last_download_path_foreigners_2026",
)


def _sync_one(pipeline_id: str, dry_run: bool) -> dict:
    if pipeline_id not in _PIPELINE_META:
        return {"pipeline": pipeline_id, "skipped": "not in S3 source map"}

    state = load_state(pipeline_id)
    if not state:
        return {"pipeline": pipeline_id, "skipped": "no state.json"}

    raw_paths = [
        Path(p) for key in _RAW_PATH_KEYS
        if (p := state.get(key)) and Path(p).exists()
    ]
    deliverable = Path(state["deliverable_path"]) if state.get("deliverable_path") else None
    if deliverable and not deliverable.exists():
        deliverable = None

    if not raw_paths and not deliverable:
        return {"pipeline": pipeline_id, "skipped": "no local files found"}

    run_dt = datetime.now()

    if dry_run:
        from etl.core.s3_upload import _s3_key, _BUCKET
        keys = []
        for p in raw_paths:
            keys.append(_s3_key(pipeline_id, p, "raw_data", run_dt))
        if deliverable:
            base = _s3_key(pipeline_id, deliverable, "transformed_data", run_dt)
            parts = base.rsplit("/", 1)
            keys.append(f"{parts[0]}/deliverable/{parts[1]}")
        print(f"  [DRY] {pipeline_id} -> {len(keys)} file(s)")
        for k in keys:
            print(f"    s3://{_BUCKET}/{k}")
        return {"pipeline": pipeline_id, "dry_run_keys": keys}

    result = upload_pipeline_files(pipeline_id, raw_paths, deliverable, run_dt)
    uploaded = result.get("uploaded", [])
    errors = result.get("errors", [])

    status = "ok" if not errors else ("partial" if uploaded else "failed")
    print(f"  [{status.upper()}] {pipeline_id} — {len(uploaded)} uploaded, {len(errors)} errors")
    for e in errors:
        print(f"    ERROR: {e}")
    return {"pipeline": pipeline_id, "uploaded": uploaded, "errors": errors}


def main():
    ap = argparse.ArgumentParser(description="Sync pipeline outputs to S3.")
    ap.add_argument("pipelines", nargs="*", help="Pipeline IDs to sync (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="Print S3 keys without uploading")
    args = ap.parse_args()

    targets = args.pipelines if args.pipelines else list_pipelines()
    targets = [t for t in targets if t in _PIPELINE_META]

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Syncing {len(targets)} pipeline(s) to S3...\n")

    results = []
    for t in tqdm(targets, desc="Syncing", unit="pipeline"):
        results.append(_sync_one(t, args.dry_run))

    total_uploaded = sum(len(r.get("uploaded", [])) for r in results)
    total_errors = sum(len(r.get("errors", [])) for r in results)
    skipped = sum(1 for r in results if "skipped" in r)

    print(f"\nDone: {total_uploaded} files uploaded, {total_errors} errors, {skipped} skipped.")


if __name__ == "__main__":
    main()
