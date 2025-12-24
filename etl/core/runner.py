# etl/core/runner.py
from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Dict, Any, List

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
    result = pipe.run(state)

    if isinstance(result, dict) and result.get("state") is not None:
        save_state(pipeline_id, result["state"])

    print(f"Status: {result.get('status', 'unknown')}")
    if isinstance(result, dict) and result.get("message"):
        print(result["message"])
