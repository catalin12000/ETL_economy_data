from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_forecast_table(html_path: Path) -> pd.DataFrame:
    """
    Extracts the EU Cyprus forecast highlight table from the downloaded HTML.

    Output columns (wide, exactly like the site):
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
        raise RuntimeError("Could not find the forecast table with first column 'Indicators'.")

    # Clean column names
    target.columns = [str(c).strip() for c in target.columns]

    # Clean values
    target["Indicators"] = target["Indicators"].astype(str).str.strip()
    
    # Identify year columns (usually 3 years)
    year_cols = [c for c in target.columns if c != "Indicators"]
    for y in year_cols:
        target[y] = pd.to_numeric(target[y], errors="coerce")

    return target
