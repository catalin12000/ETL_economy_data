from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

_BUCKET = os.getenv("S3_BUCKET", "test-data-bucket-catalin")
_REGION = os.getenv("AWS_REGION", "eu-central-1")

def _build_pipeline_meta() -> dict[str, tuple[str, str, str]]:
    """
    Build the (country, source, db_table_name) registry by introspecting every
    Pipeline class under etl/pipelines/. Each Pipeline declares these as class
    attributes — see ADR-0004.
    """
    from importlib import import_module
    result: dict[str, tuple[str, str, str]] = {}
    pipelines_root = Path(__file__).resolve().parents[1] / "pipelines"
    for pdir in sorted(pipelines_root.iterdir()):
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        if not (pdir / "pipeline.py").exists():
            continue
        try:
            mod = import_module(f"etl.pipelines.{pdir.name}.pipeline")
            cls = getattr(mod, "Pipeline", None)
            if cls is None:
                continue
            pid = getattr(cls, "pipeline_id", pdir.name)
            country = getattr(cls, "country", None)
            source = getattr(cls, "source", None)
            table = getattr(cls, "db_table_name", None)
            if country and source and table:
                result[pid] = (country, source, table)
        except Exception:
            continue
    return result


# Pipeline ID → (country_folder, source_folder, db_table_name)
# Populated at import time from Pipeline class attributes. Fallback dict below
# is kept ONLY as a backup for pipelines that fail to import.
_PIPELINE_META: dict[str, tuple[str, str, str]] = _build_pipeline_meta()

# Backup hardcoded registry (used only as a safety net if introspection fails).
_PIPELINE_META_FALLBACK: dict[str, tuple[str, str, str]] = {
    # Cyprus — CYSTAT
    "cy_01_average_monthly_earnings":              ("cy", "cystat",          "ed_average_monthly_earnings"),
    "cy_02_building_permits_by_district":          ("cy", "cystat",          "ed_building_permits_by_district"),
    "cy_03_building_permits_by_property_type":     ("cy", "cystat",          "ed_building_permits_by_property_type"),
    "cy_04_construction_index_cy":                 ("cy", "cystat",          "ed_construction_index_cy"),
    "cy_05_consumer_price_index":                  ("cy", "cystat",          "ed_consumer_price_index"),
    "cy_08_employment_cy":                         ("cy", "cystat",          "cy_08_employment_cy"),
    "cy_09_gross_value_added_sector":              ("cy", "cystat",          "ed_gross_value_added_sector"),
    "cy_12_monthly_gross_earnings_distribution":   ("cy", "cystat",          "ed_monthly_gross_earnings_distribution"),
    "cy_14_per_day_expenditure_of_tourists":       ("cy", "cystat",          "ed_per_day_expenditure_of_tourists"),
    "cy_17_tourist_arrivals_country":              ("cy", "cystat",          "ed_tourist_arrivals_country"),
    "cy_18_tourist_arrivals_revenue":              ("cy", "cystat",          "ed_tourist_arrivals_revenue"),
    # Cyprus — Central Bank of Cyprus
    "cy_13_new_loans_millions":                    ("cy", "central_bank_cy", "ed_new_loans_millions"),
    "cy_15_residential_price_indices":             ("cy", "central_bank_cy", "ed_residential_price_indices"),
    "cy_16_total_households_loans_millions":       ("cy", "central_bank_cy", "ed_total_households_loans_millions"),
    # Cyprus — DLS
    "cy_10_lro_contracts_of_sale":                 ("cy", "dls",             "ed_lro_contracts_of_sale"),
    "cy_11_lro_transfers":                         ("cy", "dls",             "ed_lro_transfers"),
    # Cyprus — Eurostat
    "cy_06_economic_forecast_cy":                  ("cy", "eurostat",        "ed_economic_forecast_cy"),
    # Greece — ELSTAT
    "ed_building_permits_table":                   ("gr", "elstat",          "ed_building_permits"),
    "ed_building_permits_by_no_of_rooms":          ("gr", "elstat",          "ed_building_permits_by_no_of_rooms"),
    "ed_construction_index":                       ("gr", "elstat",          "ed_construction_index"),
    "ed_consumer_price_index":                     ("gr", "elstat",          "ed_consumer_price_index"),
    "ed_employment":                               ("gr", "elstat",          "ed_employment"),
    "ed_gross_fixed_capital_formation":            ("gr", "elstat",          "ed_gross_fixed_capital_formation"),
    "ed_gva_by_sector":                            ("gr", "elstat",          "ed_gva_by_sector"),
    "ed_household_income_allocation":              ("gr", "elstat",          "ed_household_income_allocation"),
    "ed_housing_finances":                         ("gr", "elstat",          "ed_housing_finances"),
    "ed_imports_exports_millions":                 ("gr", "elstat",          "ed_imports_exports_millions"),
    "ed_industrial_production_index":              ("gr", "elstat",          "ed_industrial_production_index"),
    "ed_key_partners_primary_goods":               ("gr", "elstat",          "ed_key_partners_primary_goods"),
    "ed_motor_trade_turnover_index":               ("gr", "elstat",          "ed_motor_trade_turnover_index"),
    "ed_new_built_properties_per_region":          ("gr", "elstat",          "ed_new_built_properties_per_region"),
    "ed_new_establishments_building_permits":      ("gr", "elstat",          "ed_new_establishments_building_permits"),
    "ed_new_residential_building_cost_index":      ("gr", "elstat",          "ed_new_residential_building_cost_index"),
    "ed_new_residential_buildings_work_categories": ("gr", "elstat",         "ed_new_residential_buildings_work_categories"),
    "ed_retail_trade_turnover_index":              ("gr", "elstat",          "ed_retail_trade_turnover_index"),
    "ed_retail_trade_volume_index":                ("gr", "elstat",          "ed_retail_trade_volume_index"),
    "ed_services_sector_turnover_monthly_index":   ("gr", "elstat",          "ed_services_sector_turnover_monthly_index"),
    "ed_wage_growth_index":                        ("gr", "elstat",          "ed_wage_growth_index"),
    "ed_wholesale_trade_turnover_index":           ("gr", "elstat",          "ed_wholesales_turnover_index"),
    "gdp_greece":                                  ("gr", "elstat",          "gdp_greece"),
    # Greece — Bank of Greece
    "ed_apartments_price_index_table":             ("gr", "bank_of_greece",  "ed_apartments_price_index"),
    "ed_fdi_activity":                             ("gr", "bank_of_greece",  "ed_fdi_activity"),
    "ed_fdi_country":                              ("gr", "bank_of_greece",  "ed_fdi_country"),
    "ed_fdi_real_estate":                          ("gr", "bank_of_greece",  "ed_fdi_real_estate"),
    "ed_loan_amounts_millions":                    ("gr", "bank_of_greece",  "ed_loan_amounts_millions"),
    "ed_loan_interest_rates":                      ("gr", "bank_of_greece",  "ed_loan_interest_rates"),
    "ed_office_price_volume_index":                ("gr", "bank_of_greece",  "ed_office_price_volume_index"),
    "ed_residents_di_activity":                    ("gr", "bank_of_greece",  "ed_residents_di_by_activity"),
    "ed_residents_di_country":                     ("gr", "bank_of_greece",  "ed_residents_di_by_country"),
    "ed_retail_price_rental_index":                ("gr", "bank_of_greece",  "ed_retail_price_rental_index"),
    "ed_tourists_arrivals_revenue":                ("gr", "bank_of_greece",  "ed_tourists_arrivals_revenue"),
    # Greece — Eurostat
    "ed_economic_forecast":                        ("gr", "eurostat",        "ed_economic_forecast"),
    "ed_economic_sentiment_indicator":             ("gr", "eurostat",        "ed_economic_sentiment_indicator"),
    "ed_eu_consumer_confidence_index":             ("gr", "eurostat",        "ed_eu_consumer_confidence_index"),
    "ed_eu_gdp":                                   ("gr", "eurostat",        "ed_eu_gdp"),
    "ed_eu_hicp":                                  ("gr", "eurostat",        "ed_eu_harmonized_index_of_consumer_prices"),
    "ed_eu_unemployment_rate":                     ("gr", "eurostat",        "ed_eu_unemployment_rate"),
    # Greece — migration.gov.gr
    "ed_geo_distribution_of_issued_and_pending_permits": ("gr", "migration_gov", "ed_geo_distribution_of_issued_and_pending_permits"),
    "ed_residence_permits_aggregate":              ("gr", "migration_gov",   "ed_residence_permits_aggregate"),
    "ed_residence_permits_application":            ("gr", "migration_gov",   "ed_residence_permits_application"),
    "ed_residence_permits_current":                ("gr", "migration_gov",   "ed_residence_permits_current"),
    "ed_residence_permits_golden_visa":            ("gr", "migration_gov",   "ed_residence_permits_golden_visa"),
    "ed_residence_permits_issued":                 ("gr", "migration_gov",   "ed_residence_permits_issued"),
    "ed_residence_permits_top10_countries":        ("gr", "migration_gov",   "ed_residence_permits_top10_countries"),
    "ed_residence_permits_top10_countries_golden_visa": ("gr", "migration_gov", "ed_residence_permits_top10_countries_golden_visa"),
}

# Merge: fallback fills any gaps from auto-discovery (defensive — should be empty in practice).
for _pid, _meta in _PIPELINE_META_FALLBACK.items():
    _PIPELINE_META.setdefault(_pid, _meta)


def _get_client():
    return boto3.client(
        "s3",
        region_name=_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def _s3_key(
    pipeline_id: str,
    file_path: Path,
    folder: str,
    run_dt: datetime,
    use_table_name: bool = False,
) -> str:
    """
    Build S3 key: {folder}/{country}/economy_data/{source}/{YYYYMMDD}/{name}_{timestamp}.{ext}

    For deliverables (use_table_name=True): name = DB table name so the backend
    can identify which table to load the file into.
    For raw files: name = original filename stem (audit trail only).
    """
    meta = _PIPELINE_META.get(pipeline_id, ("gr", "elstat", pipeline_id))
    country, source, table_name = meta
    date_folder = run_dt.strftime("%Y%m%d")
    timestamp = run_dt.strftime("%Y%m%d%H%M%S")
    name = table_name if use_table_name else file_path.stem
    ext = file_path.suffix
    return f"{folder}/{country}/economy_data/{source}/{date_folder}/{name}_{timestamp}{ext}"


def upload_pipeline_files(
    pipeline_id: str,
    raw_paths: list[Path],
    deliverable_path: Optional[Path],
    run_dt: Optional[datetime] = None,
) -> dict[str, list[str]]:
    """
    Upload raw downloaded files and the deliverable to S3.

    Returns dict with keys 'uploaded' (list of S3 keys) and 'errors' (list of messages).
    Failures are non-fatal — logged and returned but do not raise.
    """
    if pipeline_id not in _PIPELINE_META:
        return {"uploaded": [], "errors": [f"Pipeline '{pipeline_id}' not in S3 source map."]}

    run_dt = run_dt or datetime.now()
    client = _get_client()
    uploaded: list[str] = []
    errors: list[str] = []

    # Upload raw files
    for path in raw_paths:
        if not path or not Path(path).exists():
            continue
        key = _s3_key(pipeline_id, Path(path), "raw_data", run_dt)
        try:
            client.upload_file(str(path), _BUCKET, key)
            uploaded.append(key)
        except (BotoCoreError, ClientError) as e:
            errors.append(f"raw {path.name}: {e}")

    # Upload deliverable — named after the DB table so the backend knows where to load it
    if deliverable_path and Path(deliverable_path).exists():
        key = _s3_key(pipeline_id, Path(deliverable_path), "transformed_data", run_dt, use_table_name=True)
        parts = key.rsplit("/", 1)
        key = f"{parts[0]}/deliverable/{parts[1]}"
        try:
            client.upload_file(str(deliverable_path), _BUCKET, key)
            uploaded.append(key)
        except (BotoCoreError, ClientError) as e:
            errors.append(f"deliverable {Path(deliverable_path).name}: {e}")

    return {"uploaded": uploaded, "errors": errors}
