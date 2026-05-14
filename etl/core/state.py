import json
from pathlib import Path
from typing import Any, Dict

_PIPELINES_ROOT = Path("data") / "pipelines"
# Legacy directory kept for backward-compat reads during migration
_LEGACY_STATE_DIR = Path("data/state")


def _state_path(pipeline_id: str) -> Path:
    return _PIPELINES_ROOT / pipeline_id / "state.json"


def load_state(pipeline_id: str) -> Dict[str, Any]:
    p = _state_path(pipeline_id)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    # Fallback: read from legacy data/state/ location during migration period
    legacy = _LEGACY_STATE_DIR / f"{pipeline_id}.json"
    if legacy.exists():
        return json.loads(legacy.read_text(encoding="utf-8"))
    return {}


def save_state(pipeline_id: str, state: Dict[str, Any]) -> None:
    p = _state_path(pipeline_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
