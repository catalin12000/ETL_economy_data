from __future__ import annotations

from pathlib import Path

import pandas as pd


def extract_forecast_table(html_path: Path) -> pd.DataFrame:
    """
    Extract the EU Cyprus forecast highlight table from the downloaded HTML.
    Expected shape:
      Indicators | 2025 | 2026 | 2027
    """
    html_path = Path(html_path)
    if not html_path.exists():
        raise FileNotFoundError(f"HTML file not found: {html_path}")

    html_text = html_path.read_text(encoding="utf-8", errors="replace")

    try:
        tables = pd.read_html(html_text)
    except Exception:
        tables = pd.read_html(html_text, flavor="lxml")

    if not tables:
        raise RuntimeError("No HTML tables found in the page.")

    target = None
    for t in tables:
        cols = [str(c).strip() for c in t.columns]
        if cols and cols[0].lower() == "indicators":
            target = t.copy()
            break

    if target is None:
        found_headers = [" | ".join([str(c) for c in tbl.columns]) for tbl in tables[:8]]
        raise RuntimeError(
            "Could not find the forecast table with first column 'Indicators'. "
            f"Found these table headers (first few): {found_headers}"
        )

    target.columns = [str(c).strip() for c in target.columns]
    expected = ["Indicators", "2025", "2026", "2027"]
    if target.columns.tolist() != expected:
        raise RuntimeError(f"Forecast table columns changed. Expected {expected}, got {target.columns.tolist()}")

    target["Indicators"] = target["Indicators"].astype(str).str.strip()
    for y in ["2025", "2026", "2027"]:
        target[y] = pd.to_numeric(target[y], errors="coerce")

    return target


def extract_forecast_deliverable(html_path: Path) -> pd.DataFrame:
    """
    Convert the forecast table into deliverable format:
    Year,Gdp Growth,Inflation,Unemployment,General Government Balance,
    Gross Public Debt,Current Account Balance
    """
    df = extract_forecast_table(html_path)

    indicator_map = {
        "GDP growth (%, yoy)": "Gdp Growth",
        "Inflation (%, yoy)": "Inflation",
        "Unemployment (%)": "Unemployment",
        "General government balance (% of GDP)": "General Government Balance",
        "Gross public debt (% of GDP)": "Gross Public Debt",
        "Current account balance (% of GDP)": "Current Account Balance",
    }

    year_cols = [c for c in df.columns if str(c).strip().isdigit()]
    if not year_cols:
        raise RuntimeError("No year columns found in forecast table.")

    df_idx = df.set_index("Indicators")
    missing = [k for k in indicator_map if k not in df_idx.index]
    if missing:
        raise RuntimeError(f"Missing expected indicators in forecast table: {missing}")

    rows = []
    for year in sorted(year_cols, key=lambda x: int(str(x))):
        row = {"Year": int(year)}
        for src, dst in indicator_map.items():
            row[dst] = pd.to_numeric(df_idx.at[src, year], errors="coerce")
        rows.append(row)

    out = pd.DataFrame(rows)
    value_cols = [c for c in out.columns if c != "Year"]
    for c in value_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(1)
        out[c] = out[c].map(
            lambda v: pd.NA if pd.isna(v) else (int(v) if float(v).is_integer() else float(v))
        )

    out = out[
        [
            "Year",
            "Gdp Growth",
            "Inflation",
            "Unemployment",
            "General Government Balance",
            "Gross Public Debt",
            "Current Account Balance",
        ]
    ].sort_values("Year").reset_index(drop=True)
    return out
