"""
Sync pipeline outputs to S3.

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

from etl.core.s3_upload import upload_pipeline_files, _PIPELINE_META, _BUCKET
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

W = 55  # pipeline column width


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
        from etl.core.s3_upload import _s3_key
        keys = [_s3_key(pipeline_id, p, "raw_data", run_dt) for p in raw_paths]
        if deliverable:
            base = _s3_key(pipeline_id, deliverable, "transformed_data", run_dt)
            parts = base.rsplit("/", 1)
            keys.append(f"{parts[0]}/deliverable/{parts[1]}")
        return {"pipeline": pipeline_id, "dry_run_keys": keys}

    result = upload_pipeline_files(pipeline_id, raw_paths, deliverable, run_dt)
    return {
        "pipeline": pipeline_id,
        "uploaded": result.get("uploaded", []),
        "errors": result.get("errors", []),
    }


def _print_summary(results: list[dict], dry_run: bool) -> None:
    sep  = "=" * 90
    sep2 = "-" * 90

    print(f"\n{sep}")
    print(f"  S3 SYNC SUMMARY{'  [DRY RUN]' if dry_run else ''}")
    print(sep)
    print(f"  {'PIPELINE':<{W}}  {'STATUS':<8}  {'FILES':>5}  NOTE")
    print(sep2)

    for r in results:
        pid = r["pipeline"]
        if "skipped" in r:
            print(f"  {pid:<{W}}  {'SKIP':<8}  {'':>5}  {r['skipped']}")
        elif "dry_run_keys" in r:
            keys = r["dry_run_keys"]
            print(f"  {pid:<{W}}  {'DRY':<8}  {len(keys):>5}")
            for k in keys:
                print(f"  {'':>{W}}           s3://{_BUCKET}/{k}")
        else:
            uploaded = r.get("uploaded", [])
            errors   = r.get("errors", [])
            status   = "OK" if not errors else ("PARTIAL" if uploaded else "FAILED")
            print(f"  {pid:<{W}}  {status:<8}  {len(uploaded):>5}")
            for e in errors:
                print(f"  {'':>{W}}           ERROR: {e}")

    print(sep2)
    total_up  = sum(len(r.get("uploaded", r.get("dry_run_keys", []))) for r in results)
    total_err = sum(len(r.get("errors", [])) for r in results)
    skipped   = sum(1 for r in results if "skipped" in r)
    ok        = sum(1 for r in results if "uploaded" in r and not r.get("errors"))
    print(f"  {'TOTAL':<{W}}  {'':8}  {total_up:>5}  {ok} ok  |  {skipped} skipped  |  {total_err} errors")
    print(f"{sep}\n")


def main():
    ap = argparse.ArgumentParser(description="Sync pipeline outputs to S3.")
    ap.add_argument("pipelines", nargs="*", help="Pipeline IDs to sync (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="Print S3 keys without uploading")
    args = ap.parse_args()

    targets = args.pipelines if args.pipelines else list_pipelines()
    targets = [t for t in targets if t in _PIPELINE_META]

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Syncing {len(targets)} pipeline(s) to s3://{_BUCKET} ...\n")

    results = []
    for t in tqdm(targets, desc="Uploading", unit="pipeline", ncols=80):
        results.append(_sync_one(t, args.dry_run))

    _print_summary(results, args.dry_run)


if __name__ == "__main__":
    main()
