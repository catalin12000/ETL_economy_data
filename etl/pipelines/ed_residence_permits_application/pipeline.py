from __future__ import annotations

from typing import Dict, Any

from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint


class Pipeline:
    pipeline_id = "ed_residence_permits_application"
    display_name = "Ed Residence Permits Application"
    TABLE_SPEC = "Appendix B Table 4b"

    # Pipeline that downloads the shared Appendix B PDF
    SOURCE_PIPELINE_ID = "ed_geo_distribution_of_issued_and_pending_permits"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pdf_path = get_latest_pdf_path(self.SOURCE_PIPELINE_ID)
        src_hash, src_period = get_source_fingerprint(self.SOURCE_PIPELINE_ID)

        # Skip if Appendix B PDF hasn't changed since this pipeline last ran
        if not is_new_by_hash(state.get("source_file_sha256"), src_hash):
            new_state = dict(state)
            new_state.update({
                "source_pipeline_id": self.SOURCE_PIPELINE_ID,
                "source_file_sha256": src_hash,
                "source_latest_period_seen": src_period,
                "source_pdf_path": str(pdf_path),
                "table_spec": self.TABLE_SPEC,
            })
            return {"status": "skipped", "message": "Source Appendix B PDF unchanged.", "state": new_state}

        # Later: df = extract_table_4b(pdf_path) and write CSV
        new_state = dict(state)
        new_state.update({
            "source_pipeline_id": self.SOURCE_PIPELINE_ID,
            "source_file_sha256": src_hash,
            "source_latest_period_seen": src_period,
            "source_pdf_path": str(pdf_path),
            "table_spec": self.TABLE_SPEC,
        })

        return {
            "status": "delivered",
            "message": f"Ready to extract {self.TABLE_SPEC} from shared PDF: {pdf_path}",
            "state": new_state,
        }
