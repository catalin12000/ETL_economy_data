from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine
from etl.core.download import is_new_by_hash, sha256_file
from .extract import extract_tourist_arrivals_country


class Pipeline:
    pipeline_id = "cy_17_tourist_arrivals_country"
    display_name = "Cyprus: Tourist Arrivals By Country (Monthly)"
    MIN_DELIVERABLE_YEAR = 2023

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Tourists/Monthly/2021010E.px"

    DB_SYNC_COLS = [
        "belgium",
        "bulgaria",
        "czech_republic",
        "denmark",
        "germany",
        "estonia",
        "greece",
        "spain",
        "france",
        "ireland",
        "italy",
        "latvia",
        "lithuania",
        "hungary",
        "malta",
        "netherlands",
        "austria",
        "poland",
        "romania",
        "slovakia",
        "finland",
        "sweden",
        "united_kingdom",
        "other_eu",
        "norway",
        "switzerland",
        "russia",
        "belarus",
        "ukraine",
        "serbia",
        "other_non_eu",
        "south_africa",
        "egypt",
        "other_africa",
        "united_states",
        "canada",
        "other_america",
        "kuwait",
        "bahrain",
        "united_arab_emirates",
        "saudi_arabia",
        "qatar",
        "georgia",
        "jordan",
        "armenia",
        "israel",
        "lebanon",
        "other_asia",
        "australia",
        "other_oceania",
        "not_stated",
        "total",
        "total_europe",
        "total_eu",
        "total_non_eu",
        "total_africa",
        "total_america",
        "total_asia",
        "total_oceania",
    ]

    COUNTRY_KEY_TO_DB_COL = {
        "total": "total",
        "europe": "total_europe",
        "eu_countries": "total_eu",
        "austria": "austria",
        "belgium": "belgium",
        "bulgaria": "bulgaria",
        "czech_republic": "czech_republic",
        "denmark": "denmark",
        "germany": "germany",
        "estonia": "estonia",
        "greece": "greece",
        "spain": "spain",
        "france": "france",
        "ireland": "ireland",
        "italy": "italy",
        "latvia": "latvia",
        "lithuania": "lithuania",
        "hungary": "hungary",
        "malta": "malta",
        "netherlands": "netherlands",
        "poland": "poland",
        "romania": "romania",
        "slovakia": "slovakia",
        "finland": "finland",
        "sweden": "sweden",
        "united_kingdom": "united_kingdom",
        "norway": "norway",
        "switzerland_including_liechtenstein": "switzerland",
        "russia": "russia",
        "belarus": "belarus",
        "ukraine": "ukraine",
        "serbia": "serbia",
        "africa": "total_africa",
        "egypt": "egypt",
        "south_africa": "south_africa",
        "america": "total_america",
        "united_states": "united_states",
        "canada": "canada",
        "asia": "total_asia",
        "kuwait": "kuwait",
        "bahrain": "bahrain",
        "united_arab_emirates": "united_arab_emirates",
        "saudi_arabia": "saudi_arabia",
        "qatar": "qatar",
        "jordan": "jordan",
        "armenia": "armenia",
        "israel": "israel",
        "lebanon": "lebanon",
        "oceania": "total_oceania",
        "australia": "australia",
        "not_stated": "not_stated",
    }

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for tourist arrivals by country.")

        country_var = None
        month_var = None
        for v in variables:
            code = str(v.get("code", "")).strip().upper()
            text = str(v.get("text", "")).strip().upper()
            if "COUNTRY OF USUAL RESIDENCE" in code or "COUNTRY OF USUAL RESIDENCE" in text:
                country_var = v
            elif code == "MONTH" or text == "MONTH":
                month_var = v

        if country_var is None or month_var is None:
            raise RuntimeError("Could not identify COUNTRY/MONTH metadata for tourist arrivals.")

        country_values = country_var.get("values", [])
        if not country_values:
            raise RuntimeError("No country values found in tourist arrivals metadata.")

        return {
            "query": [
                {
                    "code": country_var["code"],
                    "selection": {"filter": "item", "values": country_values},
                },
                {
                    "code": month_var["code"],
                    "selection": {"filter": "all", "values": ["*"]},
                },
            ],
            "response": {"format": "csv"},
        }

    @staticmethod
    def _nan_series_like(df: pd.DataFrame) -> pd.Series:
        return pd.Series(float("nan"), index=df.index)

    def _sum_keys(self, wide: pd.DataFrame, keys: list[str]) -> pd.Series:
        cols = [k for k in keys if k in wide.columns]
        if not cols:
            return self._nan_series_like(wide)
        return wide[cols].sum(axis=1, min_count=1)

    def _pick_keys(self, wide: pd.DataFrame, keys: list[str]) -> pd.Series:
        out = self._nan_series_like(wide)
        for k in keys:
            if k in wide.columns:
                out = out.combine_first(wide[k])
        return out

    def _to_db_wide(self, df_long: pd.DataFrame) -> pd.DataFrame:
        df = df_long.copy()
        df["arrivals_num"] = pd.to_numeric(df["Arrivals"], errors="coerce")

        wide = (
            df.pivot_table(
                index=["Year", "Month"],
                columns="Country_key",
                values="arrivals_num",
                aggfunc="first",
            )
            .reset_index()
            .rename(columns={"Year": "year", "Month": "month"})
        )

        out = pd.DataFrame({"year": wide["year"], "month": wide["month"]})

        # Direct one-to-one mappings.
        for key, db_col in self.COUNTRY_KEY_TO_DB_COL.items():
            out[db_col] = wide[key] if key in wide.columns else self._nan_series_like(wide)

        # Derived aggregates for DB schema columns not present as direct rows.
        out["other_eu"] = self._sum_keys(
            wide, ["croatia", "luxembourg", "portugal", "slovenia"]
        )
        out["total_non_eu"] = out["total_europe"] - out["total_eu"]
        out["other_non_eu"] = self._sum_keys(
            wide, ["georgia_europe", "iceland", "turkey", "federal_republic_of_yugoslavia"]
        )
        out["other_africa"] = self._sum_keys(wide, ["libya"])
        out["other_america"] = out["total_america"] - out["united_states"] - out["canada"]
        out["georgia"] = self._pick_keys(wide, ["georgia_asia", "georgia_europe", "georgia"])
        out["other_asia"] = self._sum_keys(
            wide, ["japan", "iran", "iraq", "syria", "south_korea", "china"]
        )
        out["other_oceania"] = self._sum_keys(wide, ["new_zealand"])

        for c in self.DB_SYNC_COLS:
            if c not in out.columns:
                out[c] = pd.NA

        out = out[["year", "month", *self.DB_SYNC_COLS]].sort_values(["year", "month"])
        out = out.reset_index(drop=True)
        return out

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "17"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "cystat_tourist_arrivals.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting Tourist Arrivals By Country data from CYSTAT API...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()
        out_path.write_bytes(response.content)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "api_url_used": self.API_URL,
                "file_sha256": file_hash,
                "last_download_path": str(out_path),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

        print("Extracting tourist arrivals by country data...")
        df_new = extract_tourist_arrivals_country(out_path)

        db_path = Path("data/db") / f"cy_{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"cy_{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Month", "Country_key"],
        )

        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = self._to_db_wide(df_new)
        sql_path = Path(__file__).parent / "ed_tourist_arrivals_country.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_tourist_arrivals_country",
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=self.DB_SYNC_COLS,
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )

        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

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

        target_cols = ["ID", "Year", "Month", "Country_of_origin", "Arrivals"]
        deliver_df = df_new[pd.to_numeric(df_new["Year"], errors="coerce") >= self.MIN_DELIVERABLE_YEAR].copy()
        if not deliver_df.empty:
            engine = get_engine("zeus")
            id_lookup = pd.read_sql(
                'SELECT id, year, month FROM "public"."ed_tourist_arrivals_country"',
                engine,
            )
            id_lookup.columns = [c.lower() for c in id_lookup.columns]
            id_lookup["year"] = pd.to_numeric(id_lookup["year"], errors="coerce").astype("Int64")
            id_lookup["month"] = pd.to_numeric(id_lookup["month"], errors="coerce").astype("Int64")

            deliver_df["Year"] = pd.to_numeric(deliver_df["Year"], errors="coerce").astype("Int64")
            deliver_df["Month"] = pd.to_numeric(deliver_df["Month"], errors="coerce").astype("Int64")
            deliver_df = deliver_df.merge(
                id_lookup.rename(columns={"id": "ID", "year": "Year", "month": "Month"}),
                on=["Year", "Month"],
                how="left",
            )
            deliver_df["ID"] = pd.to_numeric(deliver_df["ID"], errors="coerce").astype("Int64")
            deliver_df = deliver_df.sort_values(["Year", "Month", "Country_order"]).copy()
            deliver_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

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
                f"Extracted {len(df_new)} rows. DB (zeus) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
