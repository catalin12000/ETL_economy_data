from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest


_MOD = "etl.pipelines.ed_building_permits_table.pipeline"

_FAKE_DF = pd.DataFrame({
    "year": [2024, 2025],
    "month": [11, 1],
    "permits_number": [100, 110],
    "area": [5000.0, 5500.0],
    "volume": [15000.0, 16000.0],
})


@patch(f"{_MOD}.write_deliverable_csv")
@patch(f"{_MOD}.compare_with_postgres")
@patch(f"{_MOD}.extract_building_permits_monthly")
@patch(f"{_MOD}.sha256_file", return_value="newhash")
@patch(f"{_MOD}.download_file", return_value={"last_modified": None, "etag": None, "content_length": None, "final_url": None, "downloaded_at_utc": None})
@patch(f"{_MOD}.get_download_url_by_title", return_value="https://example.com/file.xls")
@patch(f"{_MOD}.get_latest_publication_url", return_value="https://example.com/pub")
def test_ed_building_permits_table_delivered_without_compare_csv(
    mock_pub_url, mock_dl_url, mock_download, mock_hash, mock_extract, mock_db_compare, mock_write, tmp_path
):
    mock_extract.return_value = _FAKE_DF
    mock_db_compare.return_value = {
        "status": "success", "inserted": 1, "updated": 0,
        "inserted_df": _FAKE_DF.head(1), "updated_df": pd.DataFrame(),
    }

    from etl.pipelines.ed_building_permits_table.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run({})

    assert result["status"] == "delivered"
    db_comp = result["state"].get("db_comparison", {})
    assert "missing_in_db" in db_comp
    assert "different_in_db" in db_comp
    assert "rows_before" not in result["state"]
    assert "new_rows" not in result["state"]
    assert "mock_db_snapshot_path" not in result["state"]
