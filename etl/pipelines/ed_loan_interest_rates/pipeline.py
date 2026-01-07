from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_loan_interest_rates


class Pipeline:
    pipeline_id = "ed_loan_interest_rates"
    display_name = "Housing & Consumer Loans (Interest Rates)"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/"
        "bank-deposit-and-loan-interest-rates"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "21"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Keep original extension (.xls)
        out_path = out_dir / "Rates_TABLE_1+1a.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_page": self.SOURCE_PAGE,
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # Extraction
        print(f"Extracting Loan Rates from {out_path}...")
        try:
            df_new = extract_loan_interest_rates(out_path)
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
        
        # Enforce exact column order as requested
        final_cols = [
            "Year", "Month", "Total Consumer Loans Aprc", "Total Housing Loans Aprc", 
            "Delta Interest Rate Deposits", "Weighted Average Interest Rate New Loans In Euro", 
            "Group", "Weighted Average Interest Rate", "Loan Type", "Credit Cards", 
            "Open Account Loans", "Debit Balances On Current Accounts", "Total Interest Rate", 
            "Total Collateral Guarantees Interest Rates", "Total Small Medium Enterprises Interest Rates", 
            "Floating Rate 1 Year Fixation", "Floating Rate 1 Year Rate Fixation Collateral Guarantees", 
            "Floating Rate 1 Year Rate Fixation Floating Rate", "Over 1 To 5 Years Rate Fixation", 
            "Over 5 Years Rate Fixation", "Over 5 To 10 Years Rate Fixation", "Over 10 Years Rate Fixation", 
            "Credit Lines", "Debit Balances Sight Deposits"
        ]
        
        # Ensure all columns exist (compare_csv might drop fully empty columns if configured so, 
        # though standard behavior preserves them from extracted_df)
        # Reorder columns
        snapshot_df = res.updated_df.reindex(columns=final_cols)
        deliverable_df = res.diff_df.reindex(columns=final_cols) if not res.diff_df.empty else pd.DataFrame(columns=final_cols)
        
        # Save snapshot
        snapshot_df.to_csv(out_csv_full, index=False)
        
        # Save diff (Deliverable)
        deliverable_df.to_csv(output_file, index=False)

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "deliverable_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {
                "status": "skipped",
                "message": f"No new data detected (same file SHA256).",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. {res.new_rows} new, {res.updated_cells} updates. Deliverables generated.",
            "state": new_state,
        }
