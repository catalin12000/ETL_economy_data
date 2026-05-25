"""
Smoke tests — no network, no DB, no S3.

Verifies that every pipeline module loads cleanly, has the required class
attributes, and that the S3 sync layer can build dry-run keys without error.
"""
from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_PIPELINES_DIR = _ROOT / "etl" / "pipelines"


def _pipeline_ids() -> list[str]:
    return sorted(
        d.name
        for d in _PIPELINES_DIR.iterdir()
        if d.is_dir() and (d / "pipeline.py").exists()
    )


# ── pipeline imports ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("pipeline_id", _pipeline_ids())
def test_pipeline_imports_cleanly(pipeline_id: str) -> None:
    mod = importlib.import_module(f"etl.pipelines.{pipeline_id}.pipeline")
    assert hasattr(mod, "Pipeline"), f"{pipeline_id}: no Pipeline class"


@pytest.mark.parametrize("pipeline_id", _pipeline_ids())
def test_pipeline_has_required_attributes(pipeline_id: str) -> None:
    mod = importlib.import_module(f"etl.pipelines.{pipeline_id}.pipeline")
    cls = mod.Pipeline
    for attr in ("pipeline_id", "source_type", "country", "source"):
        assert hasattr(cls, attr), f"{pipeline_id}: missing attribute '{attr}'"
    assert cls.pipeline_id == pipeline_id, (
        f"{pipeline_id}: pipeline_id mismatch: got {cls.pipeline_id!r}"
    )


# ── core modules ─────────────────────────────────────────────────────────────

def test_core_modules_import() -> None:
    for mod_path in [
        "etl.core.runner",
        "etl.core.database",
        "etl.core.download",
        "etl.core.output",
        "etl.core.paths",
        "etl.core.state",
        "etl.core.s3_upload",
        "etl.core.fingerprint",
        "etl.core.pipeline_logging",
    ]:
        importlib.import_module(mod_path)


# ── S3 sync dry-run ───────────────────────────────────────────────────────────

def test_sync_s3_dry_run_exits_cleanly() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/sync_s3.py", "--dry-run"],
        cwd=str(_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"sync_s3.py --dry-run exited {result.returncode}\n"
        f"stdout: {result.stdout[-500:]}\n"
        f"stderr: {result.stderr[-500:]}"
    )


def test_sync_s3_key_generation(tmp_path: Path) -> None:
    from unittest.mock import patch
    from scripts.sync_s3 import _sync_one

    raw = tmp_path / "fake.xls"
    deliverable = tmp_path / "deliverable_ed_wage_growth_index_May_2026.csv"
    raw.write_bytes(b"x")
    deliverable.write_bytes(b"x")

    fake_state = {
        "last_status": "delivered",
        "last_download_path": str(raw),
        "deliverable_path": str(deliverable),
    }

    with patch("scripts.sync_s3.load_state", return_value=fake_state):
        result = _sync_one("ed_wage_growth_index", dry_run=True)

    assert "dry_run_keys" in result, f"Expected dry_run_keys, got: {result}"
    keys = result["dry_run_keys"]
    assert any("raw_data" in k for k in keys), "No raw_data key generated"
    assert any("transformed_data" in k for k in keys), "No transformed_data key generated"
