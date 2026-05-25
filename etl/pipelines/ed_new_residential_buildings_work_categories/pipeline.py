from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_new_residential_buildings_work_categories


class Pipeline:
    pipeline_id = "ed_new_residential_buildings_work_categories"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_new_residential_buildings_work_categories"
    source_type = "dynamic_file"
    display_name = "Ed New Residential Buildings Work Categories (DKT63) - Quarterly"

    PUBLICATION_CODE = "DKT63"
    
    # Matching Table 04
    TARGET_TITLE_SUBSTRING = "04. Quarterly Price Indices of Work Categories in Construction (Output)"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_residential_work_categories_index.xls"

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
        df_new = extract_new_residential_buildings_work_categories(out_path)

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = pp.output / "update_report.csv"
        db_path = pp.baseline

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Quarter"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Overall Index": "overall_index",
                " Earth-moving": "earth_moving",
                " Concrete reinforced or not": "concrete_reinforced",
                " Wall-building ": "wall_building",
                " Plastering": "plastering",
                " Electrical installations": "electrical_installations",
                " Hydraulic installations": "hydraulic_installations",
                " Central heating installations": "central_heating_installations",
                " Coverings-Coatings ": "coverings_coatings",
                " Carpentry": "carpentry",
                " Iron and steel structures": "iron_steel_structures",
                " Aluminium structures ": "aluminium_structures",
                " Painting ": "painting",
                " Insulation ": "insulation",
                " Glazing ": "glazing",
                " Elevators ": "elevators",
                " Plaster structures": "plaster_structures",
                " Special installations without appliances and accessories ": (
                    "special_installations_without_appliances_accessories"
                ),
            }
        )
        sql_path = pp.sql("ed_new_residential_buildings_work_categories.sql")
        sync_cols = [
            "overall_index",
            "earth_moving",
            "concrete_reinforced",
            "wall_building",
            "plastering",
            "electrical_installations",
            "hydraulic_installations",
            "central_heating_installations",
            "coverings_coatings",
            "carpentry",
            "iron_steel_structures",
            "aluminium_structures",
            "painting",
            "insulation",
            "glazing",
            "elevators",
            "plaster_structures",
            "special_installations_without_appliances_accessories",
        ]
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "quarter"],
            sync_cols=sync_cols,
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
            "year",
            "quarter",
            "overall_index",
            "earth_moving",
            "concrete_reinforced",
            "wall_building",
            "plastering",
            "electrical_installations",
            "hydraulic_installations",
            "central_heating_installations",
            "coverings_coatings",
            "carpentry",
            "iron_steel_structures",
            "aluminium_structures",
            "painting",
            "insulation",
            "glazing",
            "elevators",
            "plaster_structures",
            "special_installations_without_appliances_accessories",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(columns={"id": "id"})
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            delta_df["year"] = pd.to_numeric(delta_df["year"], errors="coerce").astype("Int64")
            delta_df["quarter"] = pd.to_numeric(delta_df["quarter"], errors="coerce").astype("Int64")
            value_cols = [c for c in target_cols if c not in {"id", "year", "quarter"}]
            for c in value_cols:
                delta_df[c] = pd.to_numeric(delta_df[c], errors="coerce")

            delta_df = delta_df.sort_values(["year", "quarter"]).reset_index(drop=True)
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
                "rows_before": res.rows_before,
                "rows_after": res.rows_after,
                "new_rows": res.new_rows,
                "updated_cells": res.updated_cells,
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
                "delta_path": str(output_file),
                "mock_db_snapshot_path": str(out_csv_full),
            }
        )

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
            and res.new_rows == 0
            and res.updated_cells == 0
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
