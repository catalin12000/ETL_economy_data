from __future__ import annotations
from pathlib import Path

from etl.core.state import load_state as _load_state


def load_state(pipeline_id: str) -> dict:
    st = _load_state(pipeline_id)
    if not st:
        new_path = Path("etl") / "pipelines" / pipeline_id / "state.json"
        legacy_path = Path("data/state") / f"{pipeline_id}.json"
        raise FileNotFoundError(
            f"State for '{pipeline_id}' not found. Run that pipeline once first.\n"
            f"Looked in: {new_path} and {legacy_path}"
        )
    return st


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
