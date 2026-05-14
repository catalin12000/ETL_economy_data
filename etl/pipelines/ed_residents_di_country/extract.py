from __future__ import annotations

from pathlib import Path

from etl.pipelines.ed_fdi_country.extract import extract_fdi_country_sheet


def extract_residents_di_country(xls_path: Path):
    return extract_fdi_country_sheet(Path(xls_path), "GEO-OUT")

