from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.elstat import get_download_url_by_title, get_latest_publication_url
from etl.core.output import write_deliverable_csv
from .extract import extract_gfcf


class Pipeline:
    pipeline_id = "ed_gross_fixed_capital_formation"
    display_name = "Ed Gross Fixed Capital Formation (SEL81) - Quarterly"
    PUBLICATION_CODE = "SEL81"
    TARGET_TITLE_SUBSTRING = "Quarterly Gross fixed capital formation by Asset, Chain-linked volumes"
    MIN_DB_YEAR = 2018
    MIN_DELIVERABLE_YEAR = 2022

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "13"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "elstat_gross_fixed_capital_formation.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="quarterly",
            headers=headers,
        )
        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_code": self.PUBLICATION_CODE,
                "publication_url_used": pub_url,
                "download_url_used": download_url,
                "source_url_used": download_url,
                "file_sha256": file_hash,
                "downloaded_filename": out_path.name,
                "last_download_path": str(out_path),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
            }
        )

        print("Extracting gross fixed capital formation data...")
        df_all = extract_gfcf(out_path)
        df_new = df_all[pd.to_numeric(df_all["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Quarter", "Seasonally"],
        )

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Seasonally": "seasonally",
                "Total gross fixed capital formation": "total_gross_fixed_capital_formation",
                "Dwellings": "dwellings",
                "Other buildings and structures": "other_buildings_and_structures",
                "Cultivated biological resources": "cultivated_biological_resources",
                "Transport equipment": "transport_equipment",
                "Information Communication Technology (ICT) equipment": "information_communication_technology_equipment",
                "Other machinery and equipment +weapon systems": "other_machinery_and_equipment_and_weapon_systems",
                "Intellectual property products": "intellectual_property_products",
            }
        )
        sql_path = Path(__file__).parent / "ed_gross_fixed_capital_formation.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_gross_fixed_capital_formation",
            db_name="athena",
            match_cols=["year", "quarter", "seasonally"],
            sync_cols=[
                "total_gross_fixed_capital_formation",
                "dwellings",
                "other_buildings_and_structures",
                "cultivated_biological_resources",
                "transport_equipment",
                "information_communication_technology_equipment",
                "other_machinery_and_equipment_and_weapon_systems",
                "intellectual_property_products",
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

        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "ID",
            "Year",
            "Quarter",
            "Seasonally",
            "Total gross fixed capital formation",
            "Dwellings",
            "Other buildings and structures",
            "Cultivated biological resources",
            "Transport equipment",
            "Information communication technology equipment",
            "Other machinery and equipment and weapon systems",
            "Intellectual property products",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "ID",
                    "year": "Year",
                    "quarter": "Quarter",
                    "seasonally": "Seasonally",
                    "total_gross_fixed_capital_formation": "Total gross fixed capital formation",
                    "dwellings": "Dwellings",
                    "other_buildings_and_structures": "Other buildings and structures",
                    "cultivated_biological_resources": "Cultivated biological resources",
                    "transport_equipment": "Transport equipment",
                    "information_communication_technology_equipment": "Information communication technology equipment",
                    "other_machinery_and_equipment_and_weapon_systems": "Other machinery and equipment and weapon systems",
                    "intellectual_property_products": "Intellectual property products",
                }
            )

            if "ID" in delta_df.columns:
                delta_df["ID"] = pd.to_numeric(delta_df["ID"], errors="coerce").astype("Int64")
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce")
            delta_df["Quarter"] = pd.to_numeric(delta_df["Quarter"], errors="coerce")
            delta_df = delta_df.dropna(subset=["Year", "Quarter", "Seasonally"]).copy()
            delta_df = delta_df[delta_df["Year"] >= self.MIN_DELIVERABLE_YEAR].copy()
            delta_df["Year"] = delta_df["Year"].astype(int)
            delta_df["Quarter"] = delta_df["Quarter"].astype(int)

            for col in target_cols:
                if col in ("Year", "Quarter", "Seasonally"):
                    continue
                if col in delta_df.columns:
                    delta_df[col] = pd.to_numeric(delta_df[col], errors="coerce").round(0).astype("Int64")

            season_order = {"Unadjusted": 0, "Adjusted": 1}
            delta_df["_season_order"] = delta_df["Seasonally"].map(season_order).fillna(9).astype(int)
            delta_df = delta_df.sort_values(["Year", "Quarter", "_season_order"]).drop(columns=["_season_order"])
            delta_df = delta_df.reset_index(drop=True)

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
