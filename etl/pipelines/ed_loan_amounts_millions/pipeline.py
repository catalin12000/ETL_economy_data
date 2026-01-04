from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint


class Pipeline:
    pipeline_id = "ed_loan_amounts_millions"
    display_name = "Housing & Consumer Loans (Amounts) - New Business"

    SOURCE_PIPELINE_ID = "ed_loan_interest_rates"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "20"
        
        # Reuse path and hash from the master download pipeline
        try:
            xls_path = get_latest_pdf_path(self.SOURCE_PIPELINE_ID)
            src_hash, _ = get_source_fingerprint(self.SOURCE_PIPELINE_ID)
        except Exception as e:
            return {"status": "error", "message": f"Dependency error: {str(e)}", "state": state}

        if not is_new_by_hash(state.get("file_sha256"), src_hash):
            new_state = dict(state)
            new_state.update({
                "file_sha256": src_hash,
                "last_download_path": str(xls_path),
            })
            return {
                "status": "skipped",
                "message": "Source file from master pipeline unchanged.",
                "state": new_state,
            }

        new_state = dict(state)
        new_state.update({
            "file_sha256": src_hash,
            "last_download_path": str(xls_path),
        })

        return {
            "status": "delivered",
            "message": f"Source file detected: {xls_path}. Ready for extraction.",
            "state": new_state,
        }