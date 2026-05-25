from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline
from etl.core.paths import PipelinePaths

from .extract import extract_residence_permits_aggregate


class Pipeline:
    pipeline_id = "ed_residence_permits_aggregate"
    country = "gr"
    source = "migration_gov"
    db_table_name = "ed_residence_permits_aggregate"
    source_type = "scraped"
    display_name = "Ed Residence Permits Aggregate"
    TABLE_SPEC = "Appendix B Table 1"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="28",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_aggregate,
            key_cols=["year", "month"],
            target_to_db={
                "year": "year",
                "month": "month",
                "eu_citizens_of_greek_origin": "eu_citizens_of_greek_origin",
                "third_country_nationals": "third_country_nationals",
                "political_refugees": "political_refugees",
            },
            match_cols=["year", "month"],
            sync_cols=["eu_citizens_of_greek_origin", "third_country_nationals", "political_refugees"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_aggregate.sql")),
            target_cols=[
                "id",
                "month",
                "year",
                "eu_citizens_of_greek_origin",
                "third_country_nationals",
                "political_refugees",
            ],
            snapshot_backfill=True,
        )
