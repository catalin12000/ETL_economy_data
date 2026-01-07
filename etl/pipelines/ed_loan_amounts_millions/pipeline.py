from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_loan_amounts


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

        # Check if source file is new or we missed it
        # (This pipeline might run even if source is old, if we haven't extracted yet)
        # But standard logic checks hash.
        
        new_state = dict(state)
        new_state.update({
            "file_sha256": src_hash,
            "last_download_path": str(xls_path),
        })

        # Extraction
        print(f"Extracting Loan Amounts from {xls_path}...")
        try:
            df_new = extract_loan_amounts(xls_path)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Extraction failed: {str(e)}",
                "state": new_state,
            }

        # Sync with Baseline DB
        db_path = Path("data/db") / f"{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month", "Group", "Loan Type"]
        )

        # Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Save snapshot
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # Save diff (Deliverable)
        if not res.diff_df.empty:
            res.diff_df.to_csv(output_file, index=False)
        else:
            pd.DataFrame(columns=res.updated_df.columns).to_csv(output_file, index=False)

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "deliverable_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), src_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {
                "status": "skipped",
                "message": "Source file from master pipeline unchanged.",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. {res.new_rows} new, {res.updated_cells} updates. Deliverables generated.",
            "state": new_state,
        }