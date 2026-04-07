from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline

from .extract import extract_residence_permits_top10_countries_golden_visa


class Pipeline:
    pipeline_id = "ed_residence_permits_top10_countries_golden_visa"
    display_name = "Ed Residence Permits Top10 Countries Golden Visa"
    TABLE_SPEC = "Appendix B Tables 12a and 12b"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="34",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_top10_countries_golden_visa,
            key_cols=["Year", "Month", "Rank", "Country", "Type", "Applicant"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Rank": "rank",
                "Country": "country",
                "Permits": "permits",
                "Type": "type",
                "Applicant": "applicant",
            },
            match_cols=["year", "month", "rank", "country", "type", "applicant"],
            sync_cols=["permits"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_top10_countries_golden_visa.sql")),
            target_cols=["ID", "Month", "Year", "Rank", "Country", "Permits", "Type", "Applicant"],
            snapshot_backfill=True,
        )
