from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from time import perf_counter

from etl.core.pipeline_logging import emit_event, flush_pipeline_log, new_run_id, set_log_context
from etl.core.state import load_state, save_state


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


def run_one(pipeline_id: str, run_id: str | None = None) -> Dict[str, Any]:
    """
    Runs one pipeline. Returns a result dict so callers (e.g. run.py --all) can
    accumulate results and print one consolidated summary table at the end.
    """
    run_id = run_id or new_run_id()
    set_log_context(run_id=run_id, pipeline_id=pipeline_id)
    started = perf_counter()
    state: Dict[str, Any] = {}
    _safe_print(f"\n=== Running pipeline: {pipeline_id} ===")
    emit_event(stage="pipeline", event="pipeline_started", status="start")

    try:
        pipe = _load_pipeline(pipeline_id)
        emit_event(
            stage="pipeline",
            event="pipeline_loaded",
            status="success",
            country=getattr(pipe, "country", None),
            source=getattr(pipe, "source", None),
            source_type=getattr(pipe, "source_type", None),
            display_name=getattr(pipe, "display_name", None),
            db_table_name=getattr(pipe, "db_table_name", None),
        )
        state = load_state(pipeline_id)
        emit_event(
            stage="state",
            event="state_loaded",
            status="success",
            state_path=str(Path("etl") / "pipelines" / pipeline_id / "state.json"),
            state_keys=sorted(state.keys()),
            previous_status=state.get("last_status"),
            previous_success_at_utc=state.get("last_success_at_utc"),
        )
        result = pipe.run(state)
        status = result.get("status", "unknown")
        message = result.get("message", "")
        new_state = result.get("state", state)
    except Exception as e:
        status = "error"
        message = str(e)
        new_state = state
        _safe_print(f"Error: {e}")
        emit_event(
            stage="pipeline",
            event="pipeline_failed",
            status="error",
            error_type=type(e).__name__,
            error_message=str(e),
            duration_ms=round((perf_counter() - started) * 1000),
        )

    new_state["last_run_at_utc"] = datetime.now(timezone.utc).isoformat()
    new_state["last_status"] = status
    new_state["last_message"] = message
    if status in ("delivered", "verified", "skipped"):
        new_state["last_success_at_utc"] = new_state["last_run_at_utc"]

    save_state(pipeline_id, new_state)
    emit_event(
        stage="state",
        event="state_saved",
        status="success",
        state_path=str(Path("etl") / "pipelines" / pipeline_id / "state.json"),
        state_keys=sorted(new_state.keys()),
    )

    emit_event(
        stage="pipeline",
        event="pipeline_completed",
        status=status,
        message=message,
        duration_ms=round((perf_counter() - started) * 1000),
        summary=_summary_fields(new_state),
    )
    pipeline_log_path = flush_pipeline_log(status=status, pipeline_id=pipeline_id, run_id=run_id)

    return {
        "pipeline_id": pipeline_id,
        "status": status,
        "message": message,
        "state": new_state,
        "pipeline_log_path": str(pipeline_log_path) if pipeline_log_path else "",
    }


def write_run_log(
    run_id: str,
    results: List[Dict[str, Any]],
    *,
    mode: str,
    started_at: datetime | None = None,
) -> Path:
    now = datetime.now(timezone.utc)
    path = Path("etl") / "logs" / "runs" / now.strftime("%Y-%m") / f"{run_id}.log"
    path.parent.mkdir(parents=True, exist_ok=True)

    counts: Dict[str, int] = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1

    lines = [
        "=" * 88,
        "Run Log",
        f"Run ID: {run_id}",
        f"Mode: {mode}",
        f"Started: {(started_at or now).isoformat()}",
        f"Completed: {now.isoformat()}",
        "=" * 88,
        "",
        "Summary:",
        f"  total: {len(results)}",
    ]
    for status, count in sorted(counts.items()):
        lines.append(f"  {status}: {count}")

    lines.extend(["", "Pipelines:"])
    for result in sorted(results, key=lambda r: (r["status"], r["pipeline_id"])):
        fields = _summary_fields(result["state"])
        note = " ".join((result.get("message") or "").split())
        lines.append(f"  {result['pipeline_id']}")
        lines.append(f"    status: {result['status']}")
        if fields["deliverable"]:
            lines.append(f"    deliverable: {fields['deliverable']}")
        if fields["added"] is not None:
            lines.append(f"    db_added: {fields['added']}")
        if fields["updated"] is not None:
            lines.append(f"    db_updated: {fields['updated']}")
        if result.get("pipeline_log_path"):
            lines.append(f"    pipeline_run_log: {result['pipeline_log_path']}")
        if note and result["status"] != "delivered":
            lines.append(f"    note: {note}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


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
