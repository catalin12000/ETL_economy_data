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
    if state.get("last_status") == "skipped":
        return {"pipeline": pipeline_id, "skipped": state.get("last_message") or "latest pipeline run was skipped"}
    if state.get("last_status") == "error":
        return {"pipeline": pipeline_id, "skipped": "latest pipeline run errored; keeping Pipeline Run Log local"}

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
        from etl.core.s3_upload import _log_s3_key, _pipeline_log_path, _s3_key
        keys = [_s3_key(pipeline_id, p, "raw_data", run_dt) for p in raw_paths]
        if deliverable:
            deliverable_key = _s3_key(pipeline_id, deliverable, "transformed_data", run_dt, use_table_name=True)
            keys.append(deliverable_key)
            if _pipeline_log_path(pipeline_id):
                keys.append(_log_s3_key(deliverable_key))
        return {"pipeline": pipeline_id, "dry_run_keys": keys}

    result = upload_pipeline_files(pipeline_id, raw_paths, deliverable, run_dt)
    return {
        "pipeline": pipeline_id,
        "uploaded": result.get("uploaded", []),
        "errors": result.get("errors", []),
        "details": result.get("details", {}),
    }


def _result_status(result: dict) -> str:
    if "skipped" in result:
        return "SKIPPED"
    if "dry_run_keys" in result:
        return "DRY"

    uploaded = result.get("uploaded", [])
    errors = result.get("errors", [])
    details = result.get("details", {})

    if not errors:
        return "OK"
    if details.get("deliverable_uploaded"):
        return "PARTIAL"
    if uploaded and not details.get("s3_deliverable_key"):
        return "PARTIAL"
    return "FAILED"


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
    partial   = 0
    failed    = 0

    for r in results:
        pid = r["pipeline"]
        plain_status = _result_status(r)

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

            if plain_status == "OK":
                ok += 1
                status = "[green3]OK[/]"
                note   = ""
            elif plain_status == "PARTIAL":
                partial += 1
                status = "[yellow]PARTIAL[/]"
                note   = "\n".join(f"[red]{e}[/]" for e in errors)
            else:
                failed += 1
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
        f"[yellow]{partial} partial[/]   "
        f"[yellow]{skipped} skipped[/]   "
        f"[red]{failed} failed[/]   "
        f"[red]{total_err} errors[/]"
    )
    print()


def _write_sync_log(results: list[dict], *, dry_run: bool, started_at: datetime, completed_at: datetime) -> Path:
    path = Path("etl") / "logs" / "sync" / completed_at.strftime("%Y-%m") / f"{completed_at.strftime('%Y%m%dT%H%M%S')}.log"
    path.parent.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for result in results:
        status = _result_status(result)
        counts[status] = counts.get(status, 0) + 1

    lines = [
        "=" * 88,
        "Sync Log",
        f"Bucket: s3://{_BUCKET}",
        f"Started: {started_at.isoformat()}",
        f"Completed: {completed_at.isoformat()}",
        f"Dry run: {dry_run}",
        "=" * 88,
        "",
        "Summary:",
        f"  total: {len(results)}",
    ]
    for status, count in sorted(counts.items()):
        lines.append(f"  {status.lower()}: {count}")

    lines.extend(["", "Pipelines:"])
    for result in results:
        status = _result_status(result)
        lines.append(f"  {result['pipeline']}")
        lines.append(f"    status: {status}")
        if "skipped" in result:
            lines.append(f"    reason: {result['skipped']}")
        if "dry_run_keys" in result:
            lines.append("    dry_run_keys:")
            for key in result["dry_run_keys"]:
                lines.append(f"      s3://{_BUCKET}/{key}")
        if result.get("uploaded"):
            details = result.get("details", {})
            raw_keys = details.get("raw_keys") or [k for k in result["uploaded"] if k.startswith("raw_data/")]
            deliverable_key = details.get("s3_deliverable_key")
            log_key = details.get("s3_log_key")

            if deliverable_key:
                lines.append("    deliverable:")
                lines.append(f"      s3://{_BUCKET}/{deliverable_key}")
            if log_key and log_key in result["uploaded"]:
                lines.append("    pipeline_run_log:")
                lines.append(f"      s3://{_BUCKET}/{log_key}")
            if raw_keys:
                lines.append("    raw_files:")
                for key in raw_keys:
                    if key in result["uploaded"]:
                        lines.append(f"      s3://{_BUCKET}/{key}")
        if result.get("errors"):
            lines.append("    errors:")
            for error in result["errors"]:
                lines.append(f"      {error}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main():
    ap = argparse.ArgumentParser(description="Sync pipeline outputs to S3.")
    ap.add_argument("pipelines", nargs="*", help="Pipeline IDs to sync (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="Print S3 keys without uploading")
    args = ap.parse_args()

    targets = args.pipelines if args.pipelines else list_pipelines()
    targets = [t for t in targets if t in _PIPELINE_META]

    console.print(f"\n[bold yellow]Syncing {len(targets)} pipeline(s) to [white]s3://{_BUCKET}[/][/]{'[yellow]  (DRY RUN)[/]' if args.dry_run else ''}\n")

    started_at = datetime.now()
    results = []
    for t in tqdm(targets, desc="Uploading", unit="pipeline", ncols=80):
        results.append(_sync_one(t, args.dry_run))

    _print_summary(results, args.dry_run)
    _write_sync_log(results, dry_run=args.dry_run, started_at=started_at, completed_at=datetime.now())


if __name__ == "__main__":
    main()
