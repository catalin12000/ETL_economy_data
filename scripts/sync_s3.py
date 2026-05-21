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
from rich.console import Console
from rich.table import Table
from rich import box

from etl.core.s3_upload import upload_pipeline_files, _PIPELINE_META, _BUCKET
from etl.core.state import load_state
from etl.core.runner import list_pipelines

console = Console()


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
            base = _s3_key(pipeline_id, deliverable, "transformed_data", run_dt, use_table_name=True)
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
    title = "S3 Sync Summary" + (" [DRY RUN]" if dry_run else "")

    table = Table(
        title=title,
        box=box.SIMPLE_HEAD,
        title_style="bold yellow",
        header_style="bold yellow",
        border_style="grey50",
        pad_edge=True,
        show_footer=False,
    )

    table.add_column("Pipeline", style="white", min_width=48)
    table.add_column("Status",   justify="center", min_width=8)
    table.add_column("Files",    justify="right",  min_width=5)
    table.add_column("Note",     style="grey70")

    total_up  = 0
    total_err = 0
    skipped   = 0
    ok        = 0

    for r in results:
        pid = r["pipeline"]

        if "skipped" in r:
            skipped += 1
            table.add_row(pid, "[yellow]SKIP[/]", "-", r["skipped"])

        elif "dry_run_keys" in r:
            keys = r["dry_run_keys"]
            total_up += len(keys)
            table.add_row(pid, "[sky_blue1]DRY[/]", str(len(keys)), "")

        else:
            uploaded = r.get("uploaded", [])
            errors   = r.get("errors", [])
            total_up  += len(uploaded)
            total_err += len(errors)

            if not errors:
                ok += 1
                status = "[green3]OK[/]"
                note   = ""
            elif uploaded:
                status = "[yellow]PARTIAL[/]"
                note   = "\n".join(f"[red]{e}[/]" for e in errors)
            else:
                status = "[red]FAILED[/]"
                note   = "\n".join(f"[red]{e}[/]" for e in errors)

            table.add_row(pid, status, str(len(uploaded)), note)

    console.print()
    console.print(table)

    # Print dry-run keys below the table
    if dry_run:
        for r in results:
            if "dry_run_keys" in r:
                console.print(f"  [bright_white]{r['pipeline']}[/]")
                for k in r["dry_run_keys"]:
                    console.print(f"    [white]s3://{_BUCKET}/{k}[/]")

    print("-" * 70)
    console.print(
        f"  [bold white]{total_up} files{'  (dry run)' if dry_run else ' uploaded'}[/]   "
        f"[green3]{ok} ok[/]   "
        f"[yellow]{skipped} skipped[/]   "
        f"[red]{total_err} errors[/]"
    )
    print()


def main():
    ap = argparse.ArgumentParser(description="Sync pipeline outputs to S3.")
    ap.add_argument("pipelines", nargs="*", help="Pipeline IDs to sync (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="Print S3 keys without uploading")
    args = ap.parse_args()

    targets = args.pipelines if args.pipelines else list_pipelines()
    targets = [t for t in targets if t in _PIPELINE_META]

    console.print(f"\n[bold yellow]Syncing {len(targets)} pipeline(s) to [white]s3://{_BUCKET}[/][/]{'[yellow]  (DRY RUN)[/]' if args.dry_run else ''}\n")

    results = []
    for t in tqdm(targets, desc="Uploading", unit="pipeline", ncols=80):
        results.append(_sync_one(t, args.dry_run))

    _print_summary(results, args.dry_run)


if __name__ == "__main__":
    main()
