from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from etl.pipelines.ed_apartments_price_index_table.extract import extract_apartment_indices


class Pipeline:
    pipeline_id = "ed_apartments_price_index_table"
    display_name = "Ed Apartments Price Index Table"

    PDF_URL = "https://www.bankofgreece.gr/RelatedDocuments/Νέοι_Πίνακες_Τιμών_Κατοικιών_full.pdf"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        pdf_path = out_dir / "Neoi_Pinakes_Timon_Katoikion_full.pdf"

        meta = download_file(self.PDF_URL, pdf_path)
        file_hash = sha256_file(pdf_path)

        new_state = dict(state)
        new_state.update(
            {
                "file_sha256": file_hash,
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
                "last_download_path": str(pdf_path),
            }
        )

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": new_state}

        df = extract_apartment_indices(pdf_path)

        latest_year = int(df["Year"].max())
        latest_q = int(df[df["Year"] == latest_year]["Quarter"].max())
        latest_period = f"{latest_year}-Q{latest_q}"

        data_hash = dataframe_sha256(df, sort_cols=["Year", "Quarter", "Region"])
        new_state["data_sha256"] = data_hash
        new_state["latest_period_seen"] = latest_period

        if state.get("data_sha256") == data_hash:
            return {
                "status": "skipped",
                "message": "File changed, but extracted data is identical (same data SHA256).",
                "state": new_state,
            }

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Region": "region",
                "Index": "index",
                "Up To 5 Years Old Index": "up_to_5_years_old_index",
                "Over 5 Years Old Index": "over_5_years_old_index",
            }
        )
        sql_path = pp.sql("ed_apartments_price_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_apartments_price_index",
            db_name="athena",
            match_cols=["year", "quarter", "region"],
            sync_cols=["index", "up_to_5_years_old_index", "over_5_years_old_index"],
            tolerance=0.05,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        output_dir = pp.output
        db_diff_only_path = output_dir / "db_differences_only.csv"
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "ID",
            "year",
            "quarter",
            "region",
            "index",
            "up_to_5_years_old_index",
            "over_5_years_old_index",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(columns={"id": "ID"}).copy()
            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped["year"] = pd.to_numeric(shaped["year"], errors="coerce")
            shaped["quarter"] = pd.to_numeric(shaped["quarter"], errors="coerce")
            shaped = shaped.dropna(subset=["year", "quarter", "region"]).copy()
            shaped["year"] = shaped["year"].astype(int)
            shaped["quarter"] = shaped["quarter"].astype(int)
            shaped = shaped.sort_values(["year", "quarter", "region"]).reset_index(drop=True)
            shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            for column in ["index", "up_to_5_years_old_index", "over_5_years_old_index"]:
                if column in shaped.columns:
                    shaped[column] = pd.to_numeric(shaped[column], errors="coerce").map(
                        lambda x: "" if pd.isna(x) else f"{float(x):.3f}"
                    )

            for column in target_cols:
                if column not in shaped.columns:
                    shaped[column] = pd.NA

            return shaped[target_cols]

        write_deliverable_csv(shape_output(delta_df), deliverable_path)
        shape_output(updated_df).to_csv(db_diff_only_path, index=False)

        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }
        new_state.update(
            {
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
                "db_differences_only_path": str(db_diff_only_path),
            }
        )

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
            and db_comp_res.get("inserted") == 0
            and db_comp_res.get("updated") == 0
        ):
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df)} rows. DB (athena) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
