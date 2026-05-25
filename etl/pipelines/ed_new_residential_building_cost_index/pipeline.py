from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_new_residential_building_cost_index


class Pipeline:
    pipeline_id = "ed_new_residential_building_cost_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_new_residential_building_cost_index"
    source_type = "dynamic_file"
    display_name = "Ed New Residential Building Cost Index (DKT63) - Quarterly"

    # DKT63: Price Indices for New Residential Buildings Construction
    PUBLICATION_CODE = "DKT63"
    
    # Substring matching to handle changing period labels
    TARGET_TITLE_SUBSTRING = "02. Price Indices for New Residential Buildings Construction"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_residential_building_cost_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest quarterly page dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE, 
            locale="en", 
            frequency="quarterly", 
            headers=headers
        )
        
        # 2) Find download link
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE_SUBSTRING, headers=headers)

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        print(f"Extracting data from {out_path}...")
        df_new = extract_new_residential_building_cost_index(out_path)

        output_dir = pp.output


        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Overall Cost Index": "overall_cost_index",
                "Material Costs Index": "material_costs_index",
                "Labour Costs Index": "labour_costs_index",
            }
        )
        sql_path = pp.sql("ed_new_residential_building_cost_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "quarter"],
            sync_cols=["overall_cost_index", "material_costs_index", "labour_costs_index"],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )

        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "id",
            "Year",
            "Quarter",
            "Overall Cost Index",
            "Material Costs Index",
            "Labour Costs Index",
        ]
        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "quarter": "Quarter",
                    "overall_cost_index": "Overall Cost Index",
                    "material_costs_index": "Material Costs Index",
                    "labour_costs_index": "Labour Costs Index",
                }
            )
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce").astype("Int64")
            delta_df["Quarter"] = pd.to_numeric(delta_df["Quarter"], errors="coerce").astype("Int64")
            for c in target_cols[2:]:
                delta_df[c] = pd.to_numeric(delta_df[c], errors="coerce")

            delta_df = delta_df.sort_values(["Year", "Quarter"]).reset_index(drop=True)
            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
            write_deliverable_csv(delta_df[target_cols], deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }
        new_state.update(
            {
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
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
                f"Extracted {len(df_new)} rows. DB (athena) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
