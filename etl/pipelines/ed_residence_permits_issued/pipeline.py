from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline

from .extract import extract_residence_permits_issued


class Pipeline:
    pipeline_id = "ed_residence_permits_issued"
    display_name = "Ed Residence Permits Issued"
    TABLE_SPEC = "Appendix B Tables 5 and 6"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="32",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_issued,
            key_cols=["Year", "Month", "Type"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Work": "work",
                "Other": "other",
                "Family Re": "family_reunion",
                "Studies": "studies",
                "Type": "type",
            },
            match_cols=["year", "month", "type"],
            sync_cols=["work", "other", "family_reunion", "studies"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_issued.sql")),
            target_cols=["ID", "Month", "Year", "Work", "Other", "Family Re", "Studies", "Type"],
        )
