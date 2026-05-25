from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl.core.fingerprint import dataframe_sha256


SUBCATEGORY_COLS = [
    "single_houses", "buildings_with_two_housing_units", "residential_apartment_blocks",
    "residential_commercial_apartment_blocks", "cottage_apartment_complexes",
    "residencies_for_communities", "hotels", "tourist_apartments_and_villages",
    "restaurants_coffee_bars", "other_tourist_accommodation", "office_buildings",
    "wholesale_retail_buildings", "transport_communication_buildings",
    "industrial_buildings_and_warehouses", "public_entertainment_educational_medical",
    "other_non_residential_buildings", "civil_engineering", "division_of_plots",
    "road_construction",
]


def _fake_extracted_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2025, 2025],
            "month": [1, 2],
            "permits": ["Number of permits", "Area (m2)"],
            **{c: [10.0, 20.0] for c in SUBCATEGORY_COLS},
        }
    )


@pytest.fixture()
def fake_csv_bytes() -> bytes:
    return b"REFERENCE PERIOD,MEASUREMENT,value\n2025M01,Number of permits,10\n"


@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.write_deliverable_csv")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.compare_with_postgres")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.extract_building_permits_type")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.requests.post")
def test_cy_03_skips_when_data_unchanged(
    mock_post, mock_extract, mock_db_compare, mock_write, tmp_path, fake_csv_bytes
):
    mock_post.return_value = MagicMock(status_code=200, content=fake_csv_bytes)
    mock_post.return_value.raise_for_status = lambda: None

    df = _fake_extracted_df()
    mock_extract.return_value = df

    existing_hash = dataframe_sha256(df, sort_cols=["year", "month", "permits"])
    state = {"data_sha256": existing_hash}

    from etl.pipelines.cy_03_building_permits_by_property_type.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run(state)

    assert result["status"] == "skipped"
    mock_db_compare.assert_not_called()
    mock_write.assert_not_called()


@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.write_deliverable_csv")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.compare_with_postgres")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.extract_building_permits_type")
@patch("etl.pipelines.cy_03_building_permits_by_property_type.pipeline.requests.post")
def test_cy_03_saves_data_sha256_on_delivery(
    mock_post, mock_extract, mock_db_compare, mock_write, tmp_path, fake_csv_bytes
):
    mock_post.return_value = MagicMock(status_code=200, content=fake_csv_bytes)
    mock_post.return_value.raise_for_status = lambda: None

    df = _fake_extracted_df()
    mock_extract.return_value = df
    mock_db_compare.return_value = {
        "status": "success", "inserted": 2, "updated": 0,
        "inserted_df": df, "updated_df": pd.DataFrame(),
    }

    from etl.pipelines.cy_03_building_permits_by_property_type.pipeline import Pipeline
    with patch("etl.core.paths._DATA_ROOT", tmp_path / "etl" / "pipelines"):
        result = Pipeline().run({})

    assert result["status"] == "delivered"
    assert "data_sha256" in result["state"]
    expected_hash = dataframe_sha256(df, sort_cols=["year", "month", "permits"])
    assert result["state"]["data_sha256"] == expected_hash
