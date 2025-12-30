from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_forecast_table(html_path: Path) -> pd.DataFrame:
    """
    Extracts the EU Greece forecast highlight table from the downloaded HTML.

    Output columns (wide, exactly like the site):
      Indicators | 2025 | 2026 | 2027

    Raises a clear error if the table isn't found or structure changes.
    """
    html_path = Path(html_path)
    if not html_path.exists():
        raise FileNotFoundError(f"HTML file not found: {html_path}")

    html_text = html_path.read_text(encoding="utf-8", errors="replace")

    # pandas finds <table> elements and converts them to DataFrames
    # Some environments don't have lxml; try default then fallback.
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

    # Clean column names
    target.columns = [str(c).strip() for c in target.columns]

    # The table is expected to have exactly these columns (as in your screenshot)
    expected = ["Indicators", "2025", "2026", "2027"]
    if target.columns.tolist() != expected:
        raise RuntimeError(f"Forecast table columns changed. Expected {expected}, got {target.columns.tolist()}")

    # Clean values
    target["Indicators"] = target["Indicators"].astype(str).str.strip()
    for y in ["2025", "2026", "2027"]:
        target[y] = pd.to_numeric(target[y], errors="coerce")

    return target
