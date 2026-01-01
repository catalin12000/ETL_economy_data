from __future__ import annotations
from pathlib import Path
import json


def load_state(pipeline_id: str) -> dict:
    state_path = Path("data/state") / f"{pipeline_id}.json"
    if not state_path.exists():
        raise FileNotFoundError(
            f"State for '{pipeline_id}' not found. Run that pipeline once first.\n"
            f"Expected: {state_path}"
        )
    return json.loads(state_path.read_text(encoding="utf-8"))


def get_latest_pdf_path(source_pipeline_id: str) -> Path:
    st = load_state(source_pipeline_id)
    p = st.get("last_download_path")
    if not p:
        raise RuntimeError(f"State for '{source_pipeline_id}' missing 'last_download_path'.")
    pdf = Path(p)
    if not pdf.exists():
        raise FileNotFoundError(f"PDF path from state does not exist: {pdf}")
    return pdf


def get_source_fingerprint(source_pipeline_id: str) -> tuple[str | None, str | None]:
    st = load_state(source_pipeline_id)
    return st.get("file_sha256"), st.get("latest_period_seen")
