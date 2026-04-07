from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline

from .extract import extract_residence_permits_current


class Pipeline:
    pipeline_id = "ed_residence_permits_current"
    display_name = "Ed Residence Permits Current"
    TABLE_SPEC = "Appendix B Table 2a"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="30",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_current,
            key_cols=["Year", "Month"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Work": "work",
                "Other": "other",
                "Family Reunification": "family_reunion",
                "Studies": "studies",
            },
            match_cols=["year", "month"],
            sync_cols=["work", "other", "family_reunion", "studies"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_current.sql")),
            target_cols=["ID", "Month", "Year", "Work", "Other", "Family Reunification", "Studies"],
        )
