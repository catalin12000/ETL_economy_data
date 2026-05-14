from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.core.migration_appendix_b import extract_geo_distribution_of_issued_and_pending_permits as _extract


def extract_geo_distribution_of_issued_and_pending_permits(
    pdf_path: str | Path, report_year: int | None = None, report_month: int | None = None
) -> pd.DataFrame:
    return _extract(Path(pdf_path), report_year=report_year, report_month=report_month)
