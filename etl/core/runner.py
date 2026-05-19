from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from etl.core.state import load_state, save_state
from etl.core.s3_upload import upload_pipeline_files, _PIPELINE_META


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        print(str(text).encode("ascii", errors="backslashreplace").decode("ascii"))


def _pipelines_root() -> Path:
    # .../etl/core/runner.py -> .../etl/pipelines
    return Path(__file__).resolve().parents[1] / "pipelines"


def list_pipelines() -> List[str]:
    """
    Returns pipeline IDs based on folders under etl/pipelines/<pipeline_id>/pipeline.py
    """
    root = _pipelines_root()
    if not root.exists():
        return []

    out: List[str] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        if d.name.startswith("__"):
            continue
        if (d / "pipeline.py").exists():
            out.append(d.name)
    return out


def _load_pipeline(pipeline_id: str):
    mod = import_module(f"etl.pipelines.{pipeline_id}.pipeline")
    return mod.Pipeline()


def _summary_fields(state: Dict[str, Any]) -> Dict[str, Optional[int | str]]:
    """Pull the bits we want in the per-run summary out of pipeline state."""
    db = state.get("db_comparison") or {}
    deliv = state.get("deliverable_path") or ""
    deliv_name = Path(deliv).name if deliv else ""
    return {
        "deliverable": deliv_name,
        "added": db.get("missing_in_db"),
        "updated": db.get("different_in_db"),
        "local_new_rows": state.get("new_rows"),
        "local_updated_cells": state.get("updated_cells"),
    }


def run_one(pipeline_id: str) -> Dict[str, Any]:
    """
    Runs one pipeline. Returns a result dict so callers (e.g. run.py --all) can
    accumulate results and print one consolidated summary table at the end.
    """
    pipe = _load_pipeline(pipeline_id)
    state: Dict[str, Any] = load_state(pipeline_id)

    _safe_print(f"\n=== Running pipeline: {pipeline_id} ===")

    try:
        result = pipe.run(state)
        status = result.get("status", "unknown")
        message = result.get("message", "")
        new_state = result.get("state", state)
    except Exception as e:
        status = "error"
        message = str(e)
        new_state = state
        _safe_print(f"Error: {e}")

    run_dt = datetime.now()
    new_state["last_run_at_utc"] = run_dt.astimezone(timezone.utc).isoformat()
    new_state["last_status"] = status
    new_state["last_message"] = message
    if status in ("delivered", "verified", "skipped"):
        new_state["last_success_at_utc"] = new_state["last_run_at_utc"]

    save_state(pipeline_id, new_state)

    # S3 upload — only on delivered, only for pipelines in the source map
    s3_result: dict = {}
    if status == "delivered" and pipeline_id in _PIPELINE_META:
        raw_paths = [
            Path(p) for key in (
                "last_download_path",
                "last_download_path_receipts", "last_download_path_travellers",
                "last_download_path_turnover", "last_download_path_volume",
                "last_download_path_totals_2025", "last_download_path_totals_2026",
                "last_download_path_foreigners_2025", "last_download_path_foreigners_2026",
            )
            if (p := new_state.get(key))
        ]
        deliverable = Path(new_state["deliverable_path"]) if new_state.get("deliverable_path") else None
        s3_result = upload_pipeline_files(pipeline_id, raw_paths, deliverable, run_dt)
        if s3_result.get("uploaded"):
            _safe_print(f"S3: uploaded {len(s3_result['uploaded'])} file(s) to {_PIPELINE_META[pipeline_id]}")
        if s3_result.get("errors"):
            for err in s3_result["errors"]:
                _safe_print(f"S3 warning: {err}")

    return {
        "pipeline_id": pipeline_id,
        "status": status,
        "message": message,
        "state": new_state,
        "s3": s3_result,
    }


def print_run_summary(results: List[Dict[str, Any]]) -> None:
    """Final summary table — the only structured output, printed once per run."""
    if not results:
        return

    counts: Dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    # Column widths
    W_STATUS, W_PIPE, W_NUM, W_DELIV, W_ERR = 10, 55, 6, 42, 55
    total = W_STATUS + 2 + W_PIPE + 2 + W_NUM + 2 + W_NUM + 2 + W_DELIV + 2 + W_ERR
    bar = "=" * total
    sep = "-" * total

    _safe_print("")
    _safe_print(bar)
    _safe_print(
        f"Run summary  |  {len(results)} pipelines  |  "
        + "  ".join(f"{s}={n}" for s, n in sorted(counts.items()))
    )
    _safe_print(bar)
    _safe_print(
        f"{'status':<{W_STATUS}}  "
        f"{'pipeline':<{W_PIPE}}  "
        f"{'added':>{W_NUM}}  "
        f"{'upd':>{W_NUM}}  "
        f"{'deliverable':<{W_DELIV}}  "
        f"{'error / note':<{W_ERR}}"
    )
    _safe_print(sep)

    # Errors first, then delivered, then skipped, then anything else; A-Z within each group.
    def sort_key(x: Dict[str, Any]) -> tuple:
        priority = {"error": 0, "delivered": 1, "skipped": 2}.get(x["status"], 9)
        return (priority, x["pipeline_id"])

    for r in sorted(results, key=sort_key):
        f = _summary_fields(r["state"])
        added = f["added"] if f["added"] is not None else f["local_new_rows"]
        upd = f["updated"] if f["updated"] is not None else f["local_updated_cells"]
        error_note = (r["message"] or "") if r["status"] != "delivered" else ""
        # Collapse newlines so the table stays one row per pipeline.
        error_note = " ".join(error_note.split())

        _safe_print(
            f"{r['status']:<{W_STATUS}}  "
            f"{r['pipeline_id']:<{W_PIPE}.{W_PIPE}}  "
            f"{(added if added is not None else '-'):>{W_NUM}}  "
            f"{(upd if upd is not None else '-'):>{W_NUM}}  "
            f"{(f['deliverable'] or '')[:W_DELIV]:<{W_DELIV}}  "
            f"{error_note[:W_ERR]:<{W_ERR}}"
        )
    _safe_print(bar)
