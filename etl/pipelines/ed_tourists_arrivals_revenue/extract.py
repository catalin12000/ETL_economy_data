from __future__ import annotations

from pathlib import Path

import pandas as pd


AREA_MAP = {
    "Euro area countries": "Euro_area",
    "EU countries excl. euro area": "EU_countries",
    "Other countries": "Other_countries",
}

COUNTRY_MAP = {
    "Czech Republic": "Czech Rep",
    "Cruises**": "Cruises",
}

EURO_COUNTRIES = {
    "Austria",
    "Belgium",
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Cyprus",
    "Netherlands",
}

EU_OTHER_COUNTRIES = {
    "Denmark",
    "Romania",
    "Sweden",
    "Czech Republic",
}

NON_EU_COUNTRIES = {
    "Albania",
    "Australia",
    "Switzerland",
    "United Kingdom",
    "USA",
    "Canada",
    "Russia",
}

AREA_COUNTRY_MAP = {
    "Euro_area": EURO_COUNTRIES,
    "EU_countries": EU_OTHER_COUNTRIES,
    "Other_countries": NON_EU_COUNTRIES,
}


def _parse_quarter_columns(df: pd.DataFrame) -> list[tuple[int, int, int]]:
    quarter_cols: list[tuple[int, int, int]] = []
    current_year: int | None = None

    for col_idx in range(23, df.shape[1]):
        raw_year = df.iat[3, col_idx] if 3 < len(df) else None
        if pd.notna(raw_year):
            try:
                current_year = int(str(raw_year).strip().replace(".0", "").replace("*", ""))
            except Exception:
                current_year = current_year

        raw_quarter = df.iat[4, col_idx] if 4 < len(df) else None
        if current_year is None or pd.isna(raw_quarter):
            continue

        token = str(raw_quarter).strip().replace("*", "")
        if token == "II***":
            token = "II"
        quarter = {"I": 1, "II": 2, "III": 3, "IV": 4}.get(token)
        if quarter is None:
            continue
        quarter_cols.append((col_idx, current_year, quarter))

    return quarter_cols


def _extract_one(path: Path, value_col_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(path, sheet_name=0, header=None)
    quarter_cols = _parse_quarter_columns(df)
    if not quarter_cols:
        raise RuntimeError(f"No quarterly columns found in {Path(path).name}.")

    records: list[dict[str, object]] = []
    total_records: list[dict[str, object]] = []
    current_area: str | None = None

    for row_idx in range(5, len(df)):
        left = df.iat[row_idx, 0] if df.shape[1] > 0 else None
        right = df.iat[row_idx, 1] if df.shape[1] > 1 else None
        left_text = "" if pd.isna(left) else str(left).strip()
        right_text = "" if pd.isna(right) else str(right).strip()

        if left_text.startswith("Source:"):
            break

        if left_text in AREA_MAP or right_text in AREA_MAP:
            area_label = left_text if left_text in AREA_MAP else right_text
            current_area = AREA_MAP[area_label]
            for col_idx, year, quarter in quarter_cols:
                value = pd.to_numeric(df.iat[row_idx, col_idx], errors="coerce")
                if pd.isna(value):
                    continue
                total_records.append(
                    {
                        "Year": year,
                        "Quarter": quarter,
                        "Area": current_area,
                        value_col_name: float(value),
                    }
                )
            continue

        if right_text == "of which":
            continue

        if left_text in {"Cruises**", "Cruises"}:
            current_area = "Cruises"
            country = "Cruises"
        elif left_text in {"Package tours*", "Package tours"}:
            continue
        elif current_area == "Euro_area" and right_text in EURO_COUNTRIES:
            country = right_text
        elif current_area == "EU_countries" and right_text in EU_OTHER_COUNTRIES:
            country = right_text
        elif current_area == "Other_countries" and right_text in NON_EU_COUNTRIES:
            country = right_text
        else:
            continue

        country = COUNTRY_MAP.get(country, country)
        area = current_area

        for col_idx, year, quarter in quarter_cols:
            value = pd.to_numeric(df.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(value):
                continue
            records.append(
                {
                    "Year": year,
                    "Quarter": quarter,
                    "Area": area,
                    "Country of Origin": country,
                    value_col_name: float(value),
                }
            )

    detail_df = pd.DataFrame(records)
    totals_df = pd.DataFrame(total_records)
    if detail_df.empty:
        raise RuntimeError(f"No rows extracted from {Path(path).name}.")

    return detail_df, totals_df


def _append_residual_other_rows(
    detail_df: pd.DataFrame, totals_df: pd.DataFrame, value_col_name: str
) -> pd.DataFrame:
    residual_rows: list[dict[str, object]] = []

    for area, countries in AREA_COUNTRY_MAP.items():
        area_totals = totals_df[totals_df["Area"] == area][["Year", "Quarter", value_col_name]].copy()
        if area_totals.empty:
            continue

        expected_countries = [COUNTRY_MAP.get(country, country) for country in countries]
        known_source = detail_df[
            (detail_df["Area"] == area)
            & (detail_df["Country of Origin"].isin(expected_countries))
        ].copy()
        known_source[value_col_name] = known_source[value_col_name].round(1)
        known = (
            known_source.groupby(["Year", "Quarter"], as_index=False)[value_col_name]
            .sum()
            .rename(columns={value_col_name: "known_value"})
        )

        merged = area_totals.merge(known, on=["Year", "Quarter"], how="left")
        merged["known_value"] = merged["known_value"].fillna(0.0)
        merged["residual_value"] = merged[value_col_name].round(1) - merged["known_value"].round(1)

        for _, row in merged.iterrows():
            residual_value = round(float(row["residual_value"]), 1)
            if abs(residual_value) < 1e-9:
                continue
            residual_rows.append(
                {
                    "Year": int(row["Year"]),
                    "Quarter": int(row["Quarter"]),
                    "Area": area,
                    "Country of Origin": "Other",
                    value_col_name: residual_value,
                }
            )

    if not residual_rows:
        return detail_df

    residual_df = pd.DataFrame(residual_rows)
    combined = pd.concat([detail_df, residual_df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["Year", "Quarter", "Area", "Country of Origin"], keep="first"
    )
    return combined.reset_index(drop=True)


def extract_tourists_arrivals_revenue(receipts_path: Path, travellers_path: Path) -> pd.DataFrame:
    receipts_df, receipts_totals = _extract_one(
        Path(receipts_path), "Revenues by Country of Origin (millions)"
    )
    travellers_df, travellers_totals = _extract_one(
        Path(travellers_path), "Number of Travellers (000s)"
    )
    receipts_df = _append_residual_other_rows(
        receipts_df, receipts_totals, "Revenues by Country of Origin (millions)"
    )
    travellers_df = _append_residual_other_rows(
        travellers_df, travellers_totals, "Number of Travellers (000s)"
    )

    out = travellers_df.merge(
        receipts_df,
        on=["Year", "Quarter", "Area", "Country of Origin"],
        how="inner",
        validate="one_to_one",
    )
    out = out.sort_values(["Year", "Quarter", "Area", "Country of Origin"]).reset_index(drop=True)
    if out.empty:
        raise RuntimeError("No merged rows extracted for tourists arrivals/revenue.")
    return out
