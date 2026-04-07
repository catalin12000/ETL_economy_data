from __future__ import annotations

from pathlib import Path

from etl.pipelines.ed_fdi_activity.extract import extract_fdi_activity_sheet


def extract_residents_di_activity(xls_path: Path):
    out = extract_fdi_activity_sheet(Path(xls_path), "INDUSTRY-OUT").rename(
        columns={"Amount": "Amount Millions"}
    )
    return out[
        [
            "Year",
            "Section Code",
            "Section Name",
            "Subsection Code",
            "Subsection Name",
            "Amount Millions",
        ]
    ]
