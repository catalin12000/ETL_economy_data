"""Run every pipeline once and write a run_status_<timestamp>.csv summary
that matches the legacy format. Used for ad-hoc full runs."""
from __future__ import annotations

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from etl.core.runner import list_pipelines, run_one
from etl.core.state import load_state

STATE_ROOT = ROOT / "data" / "state"


def _state_summary(pipeline_id: str) -> dict[str, str]:
    state = load_state(pipeline_id)
    return {
        "pipeline": pipeline_id,
        "last_status": state.get("last_status", ""),
        "updated_at": state.get("last_run_at_utc", ""),
        "latest_deliverable_path": state.get("deliverable_path", ""),
        "last_message": state.get("last_message", ""),
    }


def main() -> int:
    pipelines = list_pipelines()
    print(f"Running {len(pipelines)} pipelines...", flush=True)
    start = time.time()
    for i, p in enumerate(pipelines, 1):
        elapsed = time.time() - start
        print(f"\n[{i}/{len(pipelines)}] (+{elapsed:.0f}s) {p}", flush=True)
        try:
            run_one(p)
        except Exception as e:
            print(f"  UNCAUGHT: {type(e).__name__}: {e}", flush=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / f"run_status_{ts}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["pipeline", "last_status", "updated_at", "latest_deliverable_path", "last_message"],
        )
        w.writeheader()
        for p in pipelines:
            w.writerow(_state_summary(p))
    total = time.time() - start
    print(f"\nDone in {total:.0f}s. Status written to {out.name}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
