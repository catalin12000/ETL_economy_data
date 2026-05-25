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
from .extract import extract_wage_growth_index


class Pipeline:
    pipeline_id = "ed_wage_growth_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_wage_growth_index"
    source_type = "dynamic_file"
    display_name = "Ed Wage Growth Index - Quarterly"

    TARGET_TITLE = "Evolution of Gross Wages and Salaries in main sections of the economy"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_wage_growth.xls"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        pub_url = get_latest_publication_url(
            publication_code="DKT03",
            locale="en",
            frequency="quarterly",
            headers=headers,
        )
        download_url = get_download_url_by_title(
            publication_url=pub_url,
            target_title=self.TARGET_TITLE,
            headers=headers,
        )

        download_note = None
        try:
            meta = download_file(download_url, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {
                "downloaded_at_utc": state.get("downloaded_at_utc"),
            }
            download_note = "Used existing local workbook because the source file was locked."

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
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
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        print("Extracting wage growth index data...")
        df_new = extract_wage_growth_index(out_path)

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
                "Mining and Quarrying": "mining_and_quarrying",
                "Manufacturing": "manufacturing",
                "Electricity, Gas, Steam and Air Conditioning Supply": "electricity_gas_steam_air_conditioning_supply",
                "Water Supply, Sewerage, Waste Management and Remediation Activities": "water_supply_sewerage_waste_management_remediation_activities",
                "Construction": "construction",
                "Wholesale and Retal Trade, Repair of Motor Vehicles and Motorcycles": "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles",
                "Transportation and Storage": "transportation_and_storage",
                "Accommodation and Food Service Activities": "accommodation_and_food_service_activities",
                "Information and Communication": "information_and_communication",
                "Professional, Scientific and Technical Activities": "professional_scientific_and_technical_activities",
                "Administrative and Support Service Activities": "administrative_and_support_service_activities",
            }
        )
        sql_path = pp.sql("ed_wage_growth_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "quarter"],
            sync_cols=[
                "mining_and_quarrying",
                "manufacturing",
                "electricity_gas_steam_air_conditioning_supply",
                "water_supply_sewerage_waste_management_remediation_activities",
                "construction",
                "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles",
                "transportation_and_storage",
                "accommodation_and_food_service_activities",
                "information_and_communication",
                "professional_scientific_and_technical_activities",
                "administrative_and_support_service_activities",
            ],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        db_diff_only_path = output_dir / "db_differences_only.csv"
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
            "mining_and_quarrying",
            "manufacturing",
            "electricity_gas_steam_air_conditioning_supply",
            "water_supply_sewerage_waste_management_remediation_activities",
            "construction",
            "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles",
            "transportation_and_storage",
            "accommodation_and_food_service_activities",
            "information_and_communication",
            "professional_scientific_and_technical_activities",
            "administrative_and_support_service_activities",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(columns={"id": "id"}).copy()
            shaped["id"] = pd.to_numeric(shaped["id"], errors="coerce")
            shaped["year"] = pd.to_numeric(shaped["year"], errors="coerce")
            shaped["quarter"] = pd.to_numeric(shaped["quarter"], errors="coerce")
            shaped = shaped.dropna(subset=["year", "quarter"]).copy()
            shaped["year"] = shaped["year"].astype(int)
            shaped["quarter"] = shaped["quarter"].astype(int)
            shaped = shaped.sort_values(["year", "quarter"]).reset_index(drop=True)
            shaped["id"] = shaped["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            value_cols = [c for c in target_cols if c not in {"id", "year", "quarter"}]
            for column in value_cols:
                if column in shaped.columns:
                    shaped[column] = pd.to_numeric(shaped[column], errors="coerce").map(
                        lambda x: "" if pd.isna(x) else f"{float(x):.1f}"
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
                "rows_before": res.rows_before,
                "rows_after": res.rows_after,
                "new_rows": res.new_rows,
                "updated_cells": res.updated_cells,
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
                "delta_path": str(output_file),
                "db_differences_only_path": str(db_diff_only_path),
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
            return {
                "status": "skipped",
                "message": f"No new data detected for {self.pipeline_id}.",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB (athena) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
                + (f" {download_note}" if download_note else "")
            ),
            "state": new_state,
        }
