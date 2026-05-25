from __future__ import annotations

from pathlib import Path

from etl.pipelines.ed_fdi_activity.extract import extract_fdi_activity_sheet


def extract_residents_di_activity(xls_path: Path):
    out = extract_fdi_activity_sheet(Path(xls_path), "INDUSTRY-OUT").rename(
        columns={"amount": "amount_millions"}
    )
    return out[
        [
            "year",
            "section_code",
            "section_name",
            "subsection_code",
            "subsection_name",
            "amount_millions",
        ]
    ]
