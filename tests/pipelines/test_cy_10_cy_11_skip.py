from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _fake_lro_df(latest_year: int = 2026, latest_month: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "year": [2025, 2025, latest_year],
        "month": [11, 12, latest_month],
        "district": ["Nicosia", "Nicosia", "Nicosia"],
        "number_parcels_total": [100, 110, 120],
        "number_parcels_locals": [80, 85, 90],
        "number_parcels_eu": [10, 12, 14],
        "number_parcels_noneu": [10, 13, 16],
    })


def _fake_lro_transfers_df(latest_year: int = 2026, latest_month: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "year": [2025, 2025, latest_year],
        "month": [11, 12, latest_month],
        "district": ["Nicosia", "Nicosia", "Nicosia"],
        "declared_price": [100.0, 110.0, 120.0],
        "accepted_price": [98.0, 108.0, 118.0],
        "number_parcels_total": [50, 55, 60],
        "number_parcels_locals": [40, 44, 48],
        "number_parcels_eu": [5, 6, 7],
        "number_parcels_non_eu": [5, 5, 5],
        "number_of_buyers_total": [50, 55, 60],
        "number_of_buyers_locals": [40, 44, 48],
        "number_of_buyers_eu": [5, 6, 7],
        "number_of_buyers_noneu": [5, 5, 5],
    })


# ── cy_10 ──────────────────────────────────────────────────────────────────────

_CY10 = "etl.pipelines.cy_10_lro_contracts_of_sale.pipeline"


@patch(f"{_CY10}.write_deliverable_csv")
@patch(f"{_CY10}.compare_with_postgres")
@patch(f"{_CY10}.extract_lro_contracts_of_sale")
@patch(f"{_CY10}.sha256_file", return_value="abc123")
@patch(f"{_CY10}.download_file", return_value={})
@patch(f"{_CY10}.Pipeline._find_page_pdf", return_value="https://example.com/contracts.pdf")
def test_cy_10_skips_when_period_unchanged(
    mock_find, mock_download, mock_hash, mock_extract, mock_db_compare, mock_write, tmp_path
):
    df = _fake_lro_df()
    mock_extract.return_value = df
    state = {"latest_period_seen": "2026-03"}

    from etl.pipelines.cy_10_lro_contracts_of_sale.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run(state)

    assert result["status"] == "skipped"
    mock_db_compare.assert_not_called()
    mock_write.assert_not_called()


@patch(f"{_CY10}.write_deliverable_csv")
@patch(f"{_CY10}.compare_with_postgres")
@patch(f"{_CY10}.extract_lro_contracts_of_sale")
@patch(f"{_CY10}.sha256_file", return_value="abc123")
@patch(f"{_CY10}.download_file", return_value={})
@patch(f"{_CY10}.Pipeline._find_page_pdf", return_value="https://example.com/contracts.pdf")
def test_cy_10_saves_latest_period_on_delivery(
    mock_find, mock_download, mock_hash, mock_extract, mock_db_compare, mock_write, tmp_path
):
    df = _fake_lro_df()
    mock_extract.return_value = df
    mock_db_compare.return_value = {
        "status": "success", "inserted": 1, "updated": 0,
        "inserted_df": df.head(1), "updated_df": pd.DataFrame(),
    }

    from etl.pipelines.cy_10_lro_contracts_of_sale.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run({})

    assert result["status"] == "delivered"
    assert result["state"].get("latest_period_seen") == "2026-03"
    db_comp = result["state"].get("db_comparison", {})
    assert "missing_in_db" in db_comp
    assert "different_in_db" in db_comp


# ── cy_11 ──────────────────────────────────────────────────────────────────────

_CY11 = "etl.pipelines.cy_11_lro_transfers.pipeline"


@patch(f"{_CY11}.write_deliverable_csv")
@patch(f"{_CY11}.compare_with_postgres")
@patch(f"{_CY11}.extract_lro_transfers")
@patch(f"{_CY11}.sha256_file", return_value="abc123")
@patch(f"{_CY11}.download_file", return_value={})
@patch(f"{_CY11}.Pipeline._find_page_pdf", return_value="https://example.com/transfers.pdf")
def test_cy_11_skips_when_period_unchanged(
    mock_find, mock_download, mock_hash, mock_extract, mock_db_compare, mock_write, tmp_path
):
    df = _fake_lro_transfers_df()
    mock_extract.return_value = df
    state = {"latest_period_seen": "2026-03"}

    from etl.pipelines.cy_11_lro_transfers.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run(state)

    assert result["status"] == "skipped"
    mock_db_compare.assert_not_called()
    mock_write.assert_not_called()


@patch(f"{_CY11}.write_deliverable_csv")
@patch(f"{_CY11}.compare_with_postgres")
@patch(f"{_CY11}.extract_lro_transfers")
@patch(f"{_CY11}.sha256_file", return_value="abc123")
@patch(f"{_CY11}.download_file", return_value={})
@patch(f"{_CY11}.Pipeline._find_page_pdf", return_value="https://example.com/transfers.pdf")
def test_cy_11_saves_latest_period_on_delivery(
    mock_find, mock_download, mock_hash, mock_extract, mock_db_compare, mock_write, tmp_path
):
    df = _fake_lro_transfers_df()
    mock_extract.return_value = df
    mock_db_compare.return_value = {
        "status": "success", "inserted": 1, "updated": 0,
        "inserted_df": df.head(1), "updated_df": pd.DataFrame(),
    }

    from etl.pipelines.cy_11_lro_transfers.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run({})

    assert result["status"] == "delivered"
    assert result["state"].get("latest_period_seen") == "2026-03"
    db_comp = result["state"].get("db_comparison", {})
    assert "missing_in_db" in db_comp
    assert "different_in_db" in db_comp
