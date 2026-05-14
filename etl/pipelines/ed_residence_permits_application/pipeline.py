from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline

from .extract import extract_residence_permits_application


class Pipeline:
    pipeline_id = "ed_residence_permits_application"
    display_name = "Ed Residence Permits Application"
    TABLE_SPEC = "Appendix B Table 4b"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="29",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_application,
            key_cols=["Year", "Month", "Type"],
            target_to_db={
                "Year": "year",
                "Month": "month",
                "Issued": "issued",
                "Rejected": "rejected",
                "Revoked": "revoked",
                "Pending": "pending",
                "Type": "type",
            },
            match_cols=["year", "month", "type"],
            sync_cols=["issued", "rejected", "revoked", "pending"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_application.sql")),
            target_cols=["ID", "Month", "Year", "Issued", "Rejected", "Revoked", "Pending", "Type"],
        )
