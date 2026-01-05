from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone

from etl.core.state import load_state, save_state


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


def run_one(pipeline_id: str) -> None:
    pipe = _load_pipeline(pipeline_id)
    state: Dict[str, Any] = load_state(pipeline_id)

    print(f"\n=== Running pipeline: {pipeline_id} ===")
    
    try:
        result = pipe.run(state)
        status = result.get("status", "unknown")
        message = result.get("message", "")
        new_state = result.get("state", state)
    except Exception as e:
        status = "error"
        message = str(e)
        new_state = state
        print(f"Error: {e}")

    # Standardize dashboard metadata
    new_state["last_run_at_utc"] = datetime.now(timezone.utc).isoformat()
    new_state["last_status"] = status
    new_state["last_message"] = message
    
    if status in ("delivered", "verified", "skipped"):
        new_state["last_success_at_utc"] = new_state["last_run_at_utc"]

    save_state(pipeline_id, new_state)

    print(f"Status: {status}")
    if message:
        print(message)
