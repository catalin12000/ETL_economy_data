import json
from pathlib import Path
from typing import Any, Dict

STATE_DIR = Path("data/state")

def load_state(pipeline_id: str) -> Dict[str, Any]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    p = STATE_DIR / f"{pipeline_id}.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))

def save_state(pipeline_id: str, state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    p = STATE_DIR / f"{pipeline_id}.json"
    p.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
