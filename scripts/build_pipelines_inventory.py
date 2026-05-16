"""
Walk etl/pipelines/ and emit a CSV inventory of every pipeline:
source, source URL, access type, file type, extractor present, DB-compare wired,
DB name + table, MIN_DELIVERABLE_YEAR.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

ROOT = Path("etl/pipelines")
OUT_PATH = Path("pipelines_inventory.csv")

URL_RE = re.compile(r"(https?://[^\s\"'\)\\]+)")
SHARED_SOURCE_PIPELINE_RE = re.compile(
    r"SOURCE_PIPELINE_ID\s*=\s*[\"']([^\"']+)[\"']"
)
TABLE_RE = re.compile(r"table_name\s*=\s*[\"']([^\"']+)[\"']")
# When the call passes table_name=self.DB_TABLE_NAME, follow the class attribute.
TABLE_ATTR_REF_RE = re.compile(r"table_name\s*=\s*self\.([A-Za-z_][A-Za-z0-9_]*)")
DB_NAME_RE = re.compile(r"db_name\s*=\s*[\"']([^\"']+)[\"']")
COMPARE_RE = re.compile(r"compare_with_postgres\s*\(")
MIN_YEAR_RE = re.compile(r"MIN_DELIVERABLE_YEAR\s*=\s*(\d+)")
DISPLAY_RE = re.compile(r"display_name\s*=\s*[\"']([^\"']+)[\"']")
PUBCODE_RE = re.compile(r"PUBLICATION_CODE\s*=\s*[\"']([^\"']+)[\"']")
INLINE_PUBCODE_RE = re.compile(
    r"get_latest_publication_url\s*\(\s*(?:publication_code\s*=\s*)?[\"']([^\"']+)[\"']"
)
OUT_PATH_FILE_RE = re.compile(
    r"out_path\s*=\s*[^\n]*?\.(xlsx|xls|csv|pdf|html|json)", re.I
)


def detect_source(urls: list[str], text: str) -> str:
    blob = (" ".join(urls) + " " + text).lower()
    if "cystat" in blob:
        return "CYSTAT"
    if "statistics.gr" in blob:
        return "ELSTAT"
    if "eurostat" in blob or "ec.europa.eu/eurostat" in blob:
        return "Eurostat"
    if "economy-finance.ec.europa.eu" in blob:
        return "EU Commission"
    if "bankofgreece" in blob:
        return "Bank of Greece"
    if "ecb.europa.eu" in blob:
        return "ECB"
    if "centralbank.cy" in blob:
        return "Central Bank of Cyprus"
    if "dls.moi.gov.cy" in blob:
        return "DLS Cyprus (Lands & Surveys)"
    if "migration.gov.gr" in blob:
        return "Greek Migration Ministry"
    if "run_shared_appendix_b_pipeline" in text or "migration_appendix_b" in text:
        return "Greek Migration Ministry"
    if "get_latest_publication_url" in text or "PUBLICATION_CODE" in text:
        return "ELSTAT"
    return ""


def detect_access_type(text: str, urls: list[str]) -> str:
    blob = (" ".join(urls) + " " + text).lower()
    if "run_shared_appendix_b_pipeline" in text:
        return "Shared (via ed_geo_distribution_of_issued_and_pending_permits)"
    if "get_latest_pdf_path" in text or "migration_source" in text:
        return "Shared (sibling pipeline)"
    if "cystatdb.cystat.gov.cy/api" in blob:
        return "API (CYSTAT PXweb)"
    if "ec.europa.eu/eurostat/api" in blob or "sdmx" in blob:
        return "API (Eurostat SDMX)"
    if (
        "get_latest_publication_url" in text
        or "get_latest_publication_year_url" in text
        or "get_download_url_by_title" in text
    ):
        return "Scrape (ELSTAT publication)"
    if "requests.post" in text:
        return "API (POST)"
    if "download_file" in text and urls:
        return "Direct URL"
    if "requests.get" in text and urls:
        return "Direct URL"
    return "Unknown"


def detect_deliverable_kind(text: str, extractor_present: bool) -> str:
    """Classify how the pipeline produces its deliverable."""
    if "run_shared_appendix_b_pipeline" in text:
        return "migration_appendix_b_csv"
    raw_copy = (
        "copy2(out_path, deliverable_path)" in text
        or 'raw_download_copy' in text
        or ("shutil.copy" in text and "deliverable_path" in text)
    )
    if raw_copy:
        return "raw_file"
    if "write_deliverable_csv" in text:
        return "extracted_csv"
    if extractor_present:
        # extract.py imported but no deliverable artifact emitted yet
        return "extracted_csv_wip"
    # Pure downloader: the raw downloaded file is the deliverable.
    return "raw_file"


def detect_file_type(text: str, urls: list[str]) -> str:
    blob = (" ".join(urls) + " " + text).lower()
    m = OUT_PATH_FILE_RE.search(text)
    if m:
        return m.group(1).lower()
    if "sdmx-csv" in blob or "format=csv" in blob:
        return "csv"
    if ".xlsx" in blob:
        return "xlsx"
    if ".xls" in blob:
        return "xls"
    if ".pdf" in blob:
        return "pdf"
    if ".html" in blob:
        return "html"
    if "response.json" in text or ".json" in blob:
        return "json"
    return ""


def first_match(rx: re.Pattern, text: str) -> str:
    m = rx.search(text)
    return m.group(1) if m else ""


def _resolve_table_name(text: str) -> str:
    """Return literal table_name, or resolve self.<ATTR> back to its string value."""
    literal = first_match(TABLE_RE, text)
    if literal:
        return literal
    attr = first_match(TABLE_ATTR_REF_RE, text)
    if not attr:
        return ""
    m = re.search(rf"{re.escape(attr)}\s*=\s*[\"']([^\"']+)[\"']", text)
    return m.group(1) if m else ""


def _collect_urls(pdir: Path) -> list[str]:
    ppy = pdir / "pipeline.py"
    if not ppy.exists():
        return []
    return URL_RE.findall(ppy.read_text(encoding="utf-8", errors="replace"))


def main() -> None:
    rows = []
    for pdir in sorted(ROOT.iterdir()):
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        ppy = pdir / "pipeline.py"
        if not ppy.exists():
            continue
        pid = pdir.name
        text = ppy.read_text(encoding="utf-8", errors="replace")
        urls = URL_RE.findall(text)

        has_extract_file = (pdir / "extract.py").exists()
        extract_imported = bool(
            re.search(
                r"from\s+\.extract\s+import"
                r"|from\s+\.\s+import\s+extract"
                rf"|from\s+etl\.pipelines\.{re.escape(pid)}\.extract\s+import",
                text,
            )
        )
        if has_extract_file and extract_imported:
            extractor = "yes"
        elif has_extract_file or extract_imported:
            extractor = "partial"
        else:
            extractor = "no"

        if pid.startswith("cy_"):
            country = "Cyprus"
        elif pid.startswith("ed_") or pid.startswith("gdp_greece"):
            country = "Greece"
        else:
            country = ""

        is_shared_migration = "run_shared_appendix_b_pipeline" in text
        shared_source_pid = first_match(SHARED_SOURCE_PIPELINE_RE, text)
        pubcode = first_match(PUBCODE_RE, text) or first_match(INLINE_PUBCODE_RE, text)

        if is_shared_migration:
            source_url = "https://migration.gov.gr/en/statistika/"
            file_type = "pdf"
        elif shared_source_pid:
            # Reuses another pipeline's downloaded file (e.g. ed_loan_amounts_millions)
            sib_urls = _collect_urls(ROOT / shared_source_pid)
            sib_text = (ROOT / shared_source_pid / "pipeline.py").read_text(
                encoding="utf-8", errors="replace"
            ) if (ROOT / shared_source_pid / "pipeline.py").exists() else ""
            source_url = sib_urls[0] if sib_urls else ""
            file_type = detect_file_type(sib_text, sib_urls)
        elif pubcode and not urls:
            # ELSTAT publication scrape — landing URL pattern from etl/core/elstat.py
            source_url = (
                f"https://www.statistics.gr/en/statistics/-/publication/{pubcode}/-"
            )
            file_type = detect_file_type(text, urls)
        else:
            source_url = urls[0] if urls else ""
            file_type = detect_file_type(text, urls)

        # Recompute source using resolved URL when the pipeline reuses a sibling's download
        effective_source_urls = [source_url] if source_url else urls
        source = detect_source(effective_source_urls, text)

        deliverable_kind = detect_deliverable_kind(text, extractor == "yes")

        rows.append({
            "pipeline_id": pid,
            "display_name": first_match(DISPLAY_RE, text),
            "country": country,
            "source": source,
            "source_url": source_url,
            "elstat_pub_code": pubcode,
            "access_type": detect_access_type(text, urls),
            "file_type": file_type,
            "has_extractor": extractor,
            "deliverable_kind": deliverable_kind,
            "db_compare_wired": "yes" if COMPARE_RE.search(text) else "no",
            "db_name": first_match(DB_NAME_RE, text),
            "db_table": _resolve_table_name(text),
            "min_deliverable_year": first_match(MIN_YEAR_RE, text),
        })

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
