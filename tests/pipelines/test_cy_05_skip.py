from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl.core.fingerprint import dataframe_sha256


def _fake_primary_df() -> pd.DataFrame:
    return pd.DataFrame({
        "year": [2025, 2025],
        "month": [1, 2],
        "index": [100.0, 101.0],
        "year_over_year": [1.0, 1.1],
    })


@pytest.fixture()
def fake_csv_bytes() -> bytes:
    return b"MONTH,BASE YEAR,Consumer Price Index\n2025M01,2025,100\n"


@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.write_deliverable_csv")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.compare_with_postgres")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.Pipeline._prepare_primary_series")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.extract_cpi")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.requests.post")
def test_cy_05_skips_when_data_unchanged(
    mock_post, mock_extract, mock_prepare, mock_db_compare, mock_write, tmp_path, fake_csv_bytes
):
    mock_post.return_value = MagicMock(status_code=200, content=fake_csv_bytes)
    mock_post.return_value.raise_for_status = lambda: None
    mock_extract.return_value = pd.DataFrame()

    df = _fake_primary_df()
    mock_prepare.return_value = df

    existing_hash = dataframe_sha256(df, sort_cols=["year", "month"])
    state = {"data_sha256": existing_hash}

    from etl.pipelines.cy_05_consumer_price_index.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run(state)

    assert result["status"] == "skipped"
    mock_db_compare.assert_not_called()
    mock_write.assert_not_called()


@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.write_deliverable_csv")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.compare_with_postgres")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.Pipeline._prepare_primary_series")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.extract_cpi")
@patch("etl.pipelines.cy_05_consumer_price_index.pipeline.requests.post")
def test_cy_05_saves_data_sha256_on_delivery(
    mock_post, mock_extract, mock_prepare, mock_db_compare, mock_write, tmp_path, fake_csv_bytes
):
    mock_post.return_value = MagicMock(status_code=200, content=fake_csv_bytes)
    mock_post.return_value.raise_for_status = lambda: None
    mock_extract.return_value = pd.DataFrame()

    df = _fake_primary_df()
    mock_prepare.return_value = df
    mock_db_compare.return_value = {
        "status": "success", "inserted": 2, "updated": 0,
        "inserted_df": df, "updated_df": pd.DataFrame(),
    }

    from etl.pipelines.cy_05_consumer_price_index.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run({})

    assert result["status"] == "delivered"
    assert "data_sha256" in result["state"]
    expected_hash = dataframe_sha256(df, sort_cols=["year", "month"])
    assert result["state"]["data_sha256"] == expected_hash
