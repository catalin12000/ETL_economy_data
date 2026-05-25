from __future__ import annotations

from unittest.mock import patch

import pytest


def _run_sync_one(pipeline_id: str, state: dict) -> dict:
    from scripts.sync_s3 import _sync_one
    with patch("scripts.sync_s3.load_state", return_value=state), \
         patch("scripts.sync_s3.upload_pipeline_files") as mock_upload:
        mock_upload.return_value = {"uploaded": [], "errors": [], "details": {}}
        return _sync_one(pipeline_id, dry_run=False), mock_upload


def test_cy_10_contracts_raw_paths_are_collected(tmp_path):
    contracts_25 = tmp_path / "contracts_2025.pdf"
    contracts_26 = tmp_path / "contracts_2026.pdf"
    foreigners_25 = tmp_path / "foreigners_2025.pdf"
    contracts_25.write_bytes(b"x")
    contracts_26.write_bytes(b"x")
    foreigners_25.write_bytes(b"x")

    state = {
        "last_status": "delivered",
        "last_download_path_contracts_2025": str(contracts_25),
        "last_download_path_contracts_2026": str(contracts_26),
        "last_download_path_foreigners_2025": str(foreigners_25),
        "deliverable_path": "",
    }
    result, mock_upload = _run_sync_one("cy_10_lro_contracts_of_sale", state)

    raw_paths_passed = [str(p) for p in mock_upload.call_args[0][1]]
    assert str(contracts_25) in raw_paths_passed
    assert str(contracts_26) in raw_paths_passed
    assert str(foreigners_25) in raw_paths_passed


def test_ed_industrial_production_raw_paths_are_collected(tmp_path):
    path_03 = tmp_path / "file_03.xls"
    path_04 = tmp_path / "file_04.xls"
    path_03.write_bytes(b"x")
    path_04.write_bytes(b"x")

    state = {
        "last_status": "delivered",
        "last_download_path_03": str(path_03),
        "last_download_path_04": str(path_04),
        "deliverable_path": "",
    }
    result, mock_upload = _run_sync_one("ed_industrial_production_index", state)

    raw_paths_passed = [str(p) for p in mock_upload.call_args[0][1]]
    assert str(path_03) in raw_paths_passed
    assert str(path_04) in raw_paths_passed


def test_ed_office_price_raw_paths_are_collected(tmp_path):
    path_price = tmp_path / "price.pdf"
    path_rent = tmp_path / "rent.pdf"
    path_price.write_bytes(b"x")
    path_rent.write_bytes(b"x")

    state = {
        "last_status": "delivered",
        "last_download_path_price": str(path_price),
        "last_download_path_rent": str(path_rent),
        "deliverable_path": "",
    }
    result, mock_upload = _run_sync_one("ed_office_price_volume_index", state)

    raw_paths_passed = [str(p) for p in mock_upload.call_args[0][1]]
    assert str(path_price) in raw_paths_passed
    assert str(path_rent) in raw_paths_passed
