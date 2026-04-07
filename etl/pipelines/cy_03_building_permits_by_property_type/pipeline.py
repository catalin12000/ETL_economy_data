from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
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

        if not delta_db_df.empty:
            delta_db_df = delta_db_df[delta_db_df["year"] >= 2023].copy()

            if not delta_db_df.empty:
                print("Transforming delta to long deliverable format...")

                cat_map = {
                    "single_houses": ("Residential Buildings", "Single houses"),
                    "buildings_with_two_housing_units": ("Residential Buildings", "Buildings with two housing units"),
                    "residential_apartment_blocks": ("Residential Buildings", "Residential apartment blocks"),
                    "residential_commercial_apartment_blocks": ("Residential Buildings", "Residential/commercial apartment blocks"),
                    "cottage_apartment_complexes": ("Residential Buildings", "Cottage apartment complexes"),
                    "residencies_for_communities": ("Residential Buildings", "Residencies for communities"),
                    "hotels": ("Non-residential Buildings", "Hotels"),
                    "tourist_apartments_and_villages": ("Non-residential Buildings", "Tourist apartments and villages"),
                    "restaurants_coffee_bars": ("Non-residential Buildings", "Restaurants, coffee-bars etc"),
                    "other_tourist_accommodation": ("Non-residential Buildings", "Other tourist accommodation"),
                    "office_buildings": ("Non-residential Buildings", "Office buildings"),
                    "wholesale_retail_buildings": ("Non-residential Buildings", "Wholesale and retail trade buildings"),
                    "transport_communication_buildings": ("Non-residential Buildings", "Transport and communication buildings"),
                    "industrial_buildings_and_warehouses": ("Non-residential Buildings", "Industrial buildings and warehouses"),
                    "public_entertainment_educational_medical": ("Non-residential Buildings", "Public entertainment, educational, medical and other institutional buildings"),
                    "other_non_residential_buildings": ("Non-residential Buildings", "Other non-residential buildings"),
                    "civil_engineering": ("Civil Engineering", "Civil engineering"),
                    "division_of_plots": ("Other", "Division of plots"),
                    "road_construction": ("Other", "Road construction"),
                }

                long_df = delta_db_df.melt(
                    id_vars=["year", "month", "permits"],
                    value_vars=[c for c in cat_map.keys() if c in delta_db_df.columns],
                    var_name="db_col",
                    value_name="val",
                )

                long_df["Type_of_project"] = long_df["db_col"].apply(lambda x: cat_map[x][0])
                long_df["Sub"] = long_df["db_col"].apply(lambda x: cat_map[x][1])

                final_deliv = long_df.pivot_table(
                    index=["year", "month", "Type_of_project", "Sub"],
                    columns="permits",
                    values="val",
                    aggfunc="first",
                ).reset_index()

                metric_renamer = {
                    "Number of permits": "Number_of_permits",
                    "Area (m2)": "Area_(m2)",
                    "Value (€000's)": "Value_(€000's)",
                    "Dwelling Units": "Dwelling_units",
                }
                final_deliv.rename(columns=metric_renamer, inplace=True)

                final_deliv.rename(
                    columns={
                        "year": "Year",
                        "month": "Month",
                        "Sub": "Type_of_project _subcategory",
                    },
                    inplace=True,
                )

                target_deliv_cols = [
                    "Year",
                    "Month",
                    "Type_of_project",
                    "Type_of_project _subcategory",
                    "Number_of_permits",
                    "Area_(m2)",
                    "Value_(€000's)",
                    "Dwelling_units",
                ]

                final_deliv = final_deliv.sort_values(
                    ["Year", "Month", "Type_of_project", "Type_of_project _subcategory"]
                ).reset_index(drop=True)

                for c in target_deliv_cols:
                    if c not in final_deliv.columns:
                        final_deliv[c] = pd.NA

                final_deliv[target_deliv_cols].to_csv(deliverable_path, index=False)
                print(f"Created deliverable in long format: {deliverable_name}")
            else:
                pd.DataFrame(columns=["Year", "Month"]).to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=["Year", "Month"]).to_csv(deliverable_path, index=False)

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
