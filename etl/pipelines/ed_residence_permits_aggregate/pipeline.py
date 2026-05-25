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
            key_cols=["Year", "Month"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Eu Citizens Of Greek Origin": "eu_citizens_of_greek_origin",
                "Third Country Nationals": "third_country_nationals",
                "Political Refugees": "political_refugees",
            },
            match_cols=["year", "month"],
            sync_cols=["eu_citizens_of_greek_origin", "third_country_nationals", "political_refugees"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_aggregate.sql")),
            target_cols=[
                "id",
                "Month",
                "Year",
                "Eu Citizens Of Greek Origin",
                "Third Country Nationals",
                "Political Refugees",
            ],
            snapshot_backfill=True,
        )
