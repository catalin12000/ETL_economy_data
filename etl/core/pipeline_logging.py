from __future__ import annotations

import contextvars
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("etl_run_id", default=None)
_PIPELINE_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("etl_pipeline_id", default=None)
_PIPELINE_EVENTS: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "etl_pipeline_events",
    default=None,
)


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{uuid4().hex[:8]}"


def set_log_context(*, run_id: str | None = None, pipeline_id: str | None = None) -> None:
    if run_id is not None:
        _RUN_ID.set(run_id)
    _PIPELINE_ID.set(pipeline_id)
    if pipeline_id is not None:
        _PIPELINE_EVENTS.set([])


def current_run_id() -> str | None:
    return _RUN_ID.get()


def current_pipeline_id() -> str | None:
    return _PIPELINE_ID.get()


def emit_event(
    *,
    stage: str,
    event: str,
    status: str = "info",
    pipeline_id: str | None = None,
    run_id: str | None = None,
    **fields: Any,
) -> None:
    rid = run_id or current_run_id()
    pid = pipeline_id if pipeline_id is not None else current_pipeline_id()
    if not rid:
        return

    now = datetime.now(timezone.utc)
    record = {
        "ts_utc": now.isoformat(),
        "run_id": rid,
        "pipeline_id": pid,
        "stage": stage,
        "event": event,
        "status": status,
        **fields,
    }
    record = _json_safe(record)

    if not pid:
        return

    events = _PIPELINE_EVENTS.get()
    if events is None:
        events = []
        _PIPELINE_EVENTS.set(events)
    events.append(record)


def flush_pipeline_log(*, status: str, pipeline_id: str | None = None, run_id: str | None = None) -> Path | None:
    rid = run_id or current_run_id()
    pid = pipeline_id if pipeline_id is not None else current_pipeline_id()
    events = _PIPELINE_EVENTS.get() or []
    if not rid or not pid or status == "skipped" or not events:
        _PIPELINE_EVENTS.set([])
        return None

    path = _pipeline_text_log_path(pid, rid, datetime.now(timezone.utc))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(_format_text_event(event) for event in events), encoding="utf-8")
    except Exception:
        # Logging must never break pipeline execution.
        return None
    finally:
        _PIPELINE_EVENTS.set([])

    return path


def _pipeline_text_log_path(pipeline_id: str, run_id: str, dt: datetime) -> Path:
    return Path("etl") / "pipelines" / pipeline_id / "logs" / dt.strftime("%Y-%m") / f"{run_id}.log"


def _format_text_event(record: dict[str, Any]) -> str:
    event = record.get("event", "")
    stage = record.get("stage", "")
    status = record.get("status", "")
    ts = record.get("ts_utc", "")

    if event == "pipeline_started":
        return (
            "=" * 88
            + "\n"
            + f"Pipeline: {record.get('pipeline_id')}\n"
            + f"Run ID:   {record.get('run_id')}\n"
            + f"Started:  {ts}\n"
            + "=" * 88
            + "\n\n"
        )

    if event == "pipeline_loaded":
        return _section(
            "Pipeline Metadata",
            ts,
            status,
            [
                ("display_name", record.get("display_name")),
                ("country", record.get("country")),
                ("source", record.get("source")),
                ("source_type", record.get("source_type")),
                ("db_table_name", record.get("db_table_name")),
            ],
        )

    if event == "state_loaded":
        return _section(
            "State Loaded",
            ts,
            status,
            [
                ("state_path", record.get("state_path")),
                ("previous_status", record.get("previous_status")),
                ("previous_success_at_utc", record.get("previous_success_at_utc")),
                ("state_keys", _join(record.get("state_keys"))),
            ],
        )

    if event == "source_downloaded":
        return _section(
            "Source Downloaded",
            ts,
            status,
            [
                ("url", record.get("url")),
                ("path", record.get("path")),
                ("bytes", record.get("bytes")),
                ("content_type", record.get("content_type")),
            ],
        )

    if event == "baseline_compare_completed":
        return _section(
            "Local Baseline Compare",
            ts,
            status,
            [
                ("baseline_path", record.get("baseline_path")),
                ("rows_before", record.get("rows_before")),
                ("rows_after", record.get("rows_after")),
                ("new_rows", record.get("new_rows")),
                ("updated_cells", record.get("updated_cells")),
                ("key_cols", _join(record.get("key_cols"))),
                ("val_cols", _join(record.get("val_cols"))),
            ],
        )

    if event == "db_schema_check_completed":
        return _section(
            "DB Schema Check",
            ts,
            status,
            [
                ("table", record.get("table")),
                ("missing_in_db", _join(record.get("missing_in_db"))),
                ("db_columns", _join(record.get("db_columns"))),
                ("extracted_columns", _join(record.get("extracted_columns"))),
            ],
        )

    if event == "db_compare_completed":
        return _section(
            "DB Compare",
            ts,
            status,
            [
                ("table", record.get("table")),
                ("db_rows", record.get("db_rows")),
                ("extracted_rows", record.get("extracted_rows")),
                ("missing_in_db", record.get("missing_in_db")),
                ("different_in_db", record.get("different_in_db")),
                ("match_cols", _join(record.get("match_cols"))),
                ("sync_cols", _join(record.get("sync_cols"))),
            ],
        )

    if event == "deliverable_written":
        return _section(
            "Deliverable Written",
            ts,
            status,
            [
                ("path", record.get("path")),
                ("rows_written", record.get("rows_written")),
                ("decimals", record.get("decimals")),
                ("columns", _join(record.get("columns"))),
            ],
        )

    if event == "state_saved":
        return _section(
            "State Saved",
            ts,
            status,
            [
                ("state_path", record.get("state_path")),
                ("state_keys", _join(record.get("state_keys"))),
            ],
        )

    if event == "pipeline_completed":
        summary = record.get("summary") or {}
        return _section(
            "Pipeline Completed",
            ts,
            status,
            [
                ("final_status", status),
                ("message", record.get("message")),
                ("duration_ms", record.get("duration_ms")),
                ("deliverable", summary.get("deliverable")),
                ("db_added", summary.get("added")),
                ("db_updated", summary.get("updated")),
                ("local_new_rows", summary.get("local_new_rows")),
                ("local_updated_cells", summary.get("local_updated_cells")),
            ],
        )

    if status == "error" or event.endswith("_failed"):
        return _section(
            "Error",
            ts,
            status,
            [
                ("stage", stage),
                ("event", event),
                ("error_type", record.get("error_type")),
                ("error_message", record.get("error_message")),
                ("duration_ms", record.get("duration_ms")),
            ],
        )

    hidden = {"ts_utc", "run_id", "pipeline_id", "stage", "event", "status"}
    return _section(
        event.replace("_", " ").title() or "Event",
        ts,
        status,
        [("stage", stage), *[(key, record.get(key)) for key in sorted(record) if key not in hidden]],
    )


def _section(title: str, ts: str, status: str, fields: list[tuple[str, Any]]) -> str:
    lines = [f"[{ts}] {title} ({status})"]
    for key, value in fields:
        if value in (None, "", []):
            continue
        lines.append(f"  {key}: {_format_value(value)}")
    return "\n".join(lines) + "\n\n"


def _format_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _join(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(v) for v in value)
    return _format_value(value) if value not in (None, "") else ""


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)
