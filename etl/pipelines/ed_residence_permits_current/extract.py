from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.core.migration_appendix_b import extract_residence_permits_current as _extract


def extract_residence_permits_current(pdf_path: str | Path, report_year: int | None = None, report_month: int | None = None) -> pd.DataFrame:
    return _extract(Path(pdf_path), report_year=report_year, report_month=report_month)
