from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from .extract import extract_building_permits_type


class Pipeline:
    pipeline_id = "cy_03_building_permits_by_property_type"
    display_name = "Cyprus: Building Permits by Property Type (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440005E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "03"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_data.csv"

        query = {
            "query": [
                {"code": "MEASUREMENT", "selection": {"filter": "item", "values": ["0"]}},
                {"code": "REFERENCE PERIOD", "selection": {"filter": "item", "values": ["0"]}},
            ],
            "response": {"format": "csv"},
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        print("Requesting data from CYSTAT API (CSV)...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()

        with open(out_path, "wb") as f:
            f.write(response.content)

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "source_url_used": self.API_URL,
                "file_sha256": file_hash,
                "last_download_path": str(out_path),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

        print("Extracting data to DB format...")
        df_new_db = extract_building_permits_type(out_path)

        db_path = Path("data/db") / f"cy_{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"cy_{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new_db,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Month", "permits"],
        )

        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        sql_path = Path(__file__).parent / "ed_building_permits_by_property_type.sql"
        all_cols = df_new_db.columns.tolist()
        sync_cols = [c for c in all_cols if c.lower() not in ["year", "month", "permits"]]

        db_comp_res = compare_with_postgres(
            df=df_new_db,
            table_name="ed_building_permits_by_property_type",
            db_name="zeus",
            match_cols=["year", "month", "permits"],
            sync_cols=sync_cols,
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )
        print(
            f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, "
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
        delta_db_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        subcategory_cols = [
            "single_houses",
            "buildings_with_two_housing_units",
            "residential_apartment_blocks",
            "residential_commercial_apartment_blocks",
            "cottage_apartment_complexes",
            "residencies_for_communities",
            "hotels",
            "tourist_apartments_and_villages",
            "restaurants_coffee_bars",
            "other_tourist_accommodation",
            "office_buildings",
            "wholesale_retail_buildings",
            "transport_communication_buildings",
            "industrial_buildings_and_warehouses",
            "public_entertainment_educational_medical",
            "other_non_residential_buildings",
            "civil_engineering",
            "division_of_plots",
            "road_construction",
        ]
        target_cols = ["ID", "Year", "Month", "permits"] + subcategory_cols

        if not delta_db_df.empty:
            delta_db_df = delta_db_df.rename(columns={"id": "ID", "year": "Year", "month": "Month"})
            delta_db_df = delta_db_df[delta_db_df["Year"] >= 2023].copy()

        if not delta_db_df.empty:
            if "ID" in delta_db_df.columns:
                delta_db_df["ID"] = pd.to_numeric(delta_db_df["ID"], errors="coerce").astype("Int64")
            else:
                delta_db_df["ID"] = pd.NA
            delta_db_df["Year"] = pd.to_numeric(delta_db_df["Year"], errors="coerce").astype("Int64")
            delta_db_df["Month"] = pd.to_numeric(delta_db_df["Month"], errors="coerce").astype("Int64")

            for c in subcategory_cols:
                if c in delta_db_df.columns:
                    delta_db_df[c] = pd.to_numeric(delta_db_df[c], errors="coerce")

            measure_order = {
                "Number of permits": 0,
                "Area (m2)": 1,
                "Value (€000's)": 2,
                "Dwelling Units": 3,
            }
            delta_db_df["__m_ord"] = delta_db_df.get("permits", pd.Series([], dtype=object)).map(measure_order).fillna(99)
            delta_db_df = delta_db_df.sort_values(["Year", "Month", "__m_ord"]).drop(columns="__m_ord").reset_index(drop=True)

            for c in target_cols:
                if c not in delta_db_df.columns:
                    delta_db_df[c] = pd.NA
            write_deliverable_csv(delta_db_df[target_cols], deliverable_path)
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

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new_db)} rows. DB Comparison result ready. File: {deliverable_name}",
            "state": new_state,
        }
