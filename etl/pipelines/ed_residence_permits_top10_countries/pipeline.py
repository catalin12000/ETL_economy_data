from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline
from etl.core.paths import PipelinePaths

from .extract import extract_residence_permits_top10_countries


class Pipeline:
    pipeline_id = "ed_residence_permits_top10_countries"
    country = "gr"
    source = "migration_gov"
    db_table_name = "ed_residence_permits_top10_countries"
    source_type = "scraped"
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
            key_cols=["year", "month", "rank", "country"],
            target_to_db={
                "year": "year",
                "month": "month",
                "rank": "rank",
                "country": "country",
                "permits_granted_to_men": "permits_granted_to_men",
                "permits_granted_to_women": "permits_granted_to_women",
                "total_permits_granted": "total_permits_granted",
            },
            match_cols=["year", "month", "rank", "country"],
            sync_cols=["total_permits_granted", "permits_granted_to_men", "permits_granted_to_women"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_top10_countries.sql")),
            target_cols=[
                "id",
                "month",
                "year",
                "rank",
                "country",
                "permits_granted_to_men",
                "permits_granted_to_women",
                "total_permits_granted",
            ],
            snapshot_backfill=True,
        )
