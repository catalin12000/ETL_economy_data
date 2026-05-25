from __future__ import annotations

from pathlib import Path
from typing import Any

from etl.core.migration_appendix_b_runner import run_shared_appendix_b_pipeline
from etl.core.paths import PipelinePaths

from .extract import extract_residence_permits_golden_visa


class Pipeline:
    pipeline_id = "ed_residence_permits_golden_visa"
    country = "gr"
    source = "migration_gov"
    db_table_name = "ed_residence_permits_golden_visa"
    source_type = "scraped"
    display_name = "Ed Residence Permits Golden Visa"
    TABLE_SPEC = "Appendix B Table 13b"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        return run_shared_appendix_b_pipeline(
            state=state,
            prefix="31",
            pipeline_id=self.pipeline_id,
            table_spec=self.TABLE_SPEC,
            extractor=extract_residence_permits_golden_visa,
            key_cols=["year", "month", "type"],
            target_to_db={
                "year": "year",
                "month": "month",
                "issued": "issued",
                "rejected": "rejected",
                "revoked": "revoked",
                "pending": "pending",
                "type": "type",
            },
            match_cols=["year", "month", "type"],
            sync_cols=["issued", "rejected", "revoked", "pending"],
            sql_file_path=str(Path(__file__).with_name("ed_residence_permits_golden_visa.sql")),
            target_cols=["id", "month", "year", "issued", "rejected", "revoked", "pending", "type"],
        )
