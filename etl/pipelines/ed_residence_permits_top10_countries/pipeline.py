from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline
from etl.core.paths import PipelinePaths

from .extract import extract_residence_permits_top10_countries


class Pipeline:
    pipeline_id = "ed_residence_permits_top10_countries"
    display_name = "Ed Residence Permits Top10 Countries"
    TABLE_SPEC = "Appendix B Table 3"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="33",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_top10_countries,
            key_cols=["Year", "Month", "Rank", "Country"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Rank": "rank",
                "Country": "country",
                "Permits_Granted_to_Men": "permits_granted_to_men",
                "Permit_Granted_to_Women": "permits_granted_to_women",
                "Total_Permits_Granted": "total_permits_granted",
            },
            match_cols=["year", "month", "rank", "country"],
            sync_cols=["total_permits_granted", "permits_granted_to_men", "permits_granted_to_women"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_top10_countries.sql")),
            target_cols=[
                "ID",
                "Month",
                "Year",
                "Rank",
                "Country",
                "Permits_Granted_to_Men",
                "Permit_Granted_to_Women",
                "Total_Permits_Granted",
            ],
            snapshot_backfill=True,
        )
