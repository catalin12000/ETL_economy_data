from __future__ import annotations

import re
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths

from .extract import extract_geo_distribution_of_issued_and_pending_permits


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._in_a = False
        self._href = ""
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._in_a = True
            self._href = dict(attrs).get("href", "") or ""
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_a:
            text = (data or "").strip()
            if text:
                self._text_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._in_a:
            text = " ".join(self._text_parts).strip()
            href = self._href.strip()
            if href and text:
                self.links.append((href, text))
            self._in_a = False
            self._href = ""
            self._text_parts = []


_MONTHS_GR = {
    "Ιανουάριος": 1,
    "Φεβρουάριος": 2,
    "Μάρτιος": 3,
    "Απρίλιος": 4,
    "Μάιος": 5,
    "Ιούνιος": 6,
    "Ιούλιος": 7,
    "Αύγουστος": 8,
    "Σεπτέμβριος": 9,
    "Οκτώβριος": 10,
    "Νοέμβριος": 11,
    "Δεκέμβριος": 12,
}


def _abs_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return "https://migration.gov.gr" + href
    return "https://migration.gov.gr/" + href.lstrip("/")


def _parse_month_year_from_title(title: str) -> tuple[int, int] | None:
    match = re.search(r"(Ιανουάριος|Φεβρουάριος|Μάρτιος|Απρίλιος|Μάιος|Ιούνιος|Ιούλιος|Αύγουστος|Σεπτέμβριος|Οκτώβριος|Νοέμβριος|Δεκέμβριος)\s+(\d{4})", title)
    if not match:
        return None
    return int(match.group(2)), _MONTHS_GR[match.group(1)]


def resolve_latest_migration_appendix_b_pdf_url(index_url: str, headers: dict[str, str]) -> tuple[str, str]:
    response = requests.get(index_url, headers=headers, timeout=60)
    response.raise_for_status()
    parser = _LinkParser()
    parser.feed(response.text)

    candidates: list[tuple[int, int, str]] = []
    for href, text in parser.links:
        if "Παράρτημα Β" not in text:
            continue
        period = _parse_month_year_from_title(text)
        if not period:
            continue
        year, month = period
        candidates.append((year, month, href))

    if not candidates:
        raise RuntimeError(f"Could not resolve Appendix B PDF from {index_url}")

    year, month, href = max(candidates, key=lambda item: (item[0], item[1]))
    return _abs_url(href), f"{year}-{month:02d}"


def _latest_db_period(table_name: str) -> tuple[int | None, int | None]:
    engine = get_engine("athena")
    result = pd.read_sql(f'SELECT MAX(year * 100 + month) AS period_key FROM "public"."{table_name}"', engine)
    value = result.iloc[0, 0]
    if pd.isna(value):
        return None, None
    value = int(value)
    return value // 100, value % 100


def _parse_archive_period(path: Path) -> tuple[int | None, int | None]:
    parts = path.stem.rsplit("_", 2)
    if len(parts) < 3:
        return None, None
    try:
        return int(parts[-2]), int(parts[-1])
    except ValueError:
        return None, None


class Pipeline:
    pipeline_id = "ed_geo_distribution_of_issued_and_pending_permits"
    display_name = "Ed Geo Distribution of Issued and Pending Permits"

    INDEX_URL = "https://migration.gov.gr/en/statistika/"
    TABLE_SPEC = "Appendix B Tables 4c and 4d"

    def run(self, state: dict[str, Any]) -> dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        pdf_path = out_dir / "migration_appendix_b.pdf"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        pdf_url, period = resolve_latest_migration_appendix_b_pdf_url(self.INDEX_URL, headers=headers)

        meta = download_file(pdf_url, pdf_path, headers=headers)
        file_hash = sha256_file(pdf_path)
        report_year, report_month = [int(part) for part in period.split("-")]

        latest_db_year, latest_db_month = _latest_db_period(self.pipeline_id)
        latest_db_key = (latest_db_year or 0) * 100 + (latest_db_month or 0)
        sources: list[tuple[int, int, Path]] = []
        if report_year * 100 + report_month > latest_db_key:
            sources.append((report_year, report_month, pdf_path))

        archive_dir = pdf_path.parent / "archive"
        if archive_dir.exists():
            for archived_pdf in archive_dir.glob("migration_appendix_b_*.pdf"):
                year, month = _parse_archive_period(archived_pdf)
                if year is None or month is None:
                    continue
                if year * 100 + month > latest_db_key:
                    sources.append((year, month, archived_pdf))

        frames: list[pd.DataFrame] = []
        seen_periods: set[tuple[int, int]] = set()
        for year, month, source_pdf in sorted(sources, key=lambda item: (item[0], item[1])):
            key = (year, month)
            if key in seen_periods:
                continue
            seen_periods.add(key)
            frames.append(
                extract_geo_distribution_of_issued_and_pending_permits(
                    source_pdf, report_year=year, report_month=month
                )
            )
        if frames:
            df_new = pd.concat(frames, ignore_index=True)
        else:
            df_new = extract_geo_distribution_of_issued_and_pending_permits(
                pdf_path, report_year=report_year, report_month=report_month
            )

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = pp.output / "update_report.csv"
        db_path = pp.baseline

        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month", "Permit Type", "Period", "Area"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Month": "month",
                "Permit Type": "permit_type",
                "Period": "period",
                "Area": "area",
                "Issued": "issued",
                "Rejected": "rejected",
                "Revoked": "revoked",
                "Pending": "pending",
            }
        )

        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month", "permit_type", "period", "area"],
            sync_cols=["issued", "rejected", "revoked", "pending"],
            tolerance=0.11,
            sql_file_path=str(Path(__file__).with_name("ed_geo_distribution_of_issued_and_pending_permits.sql")),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": dict(state)}

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        target_cols = ["id", "Year", "Month", "Permit Type", "Period", "Area", "Issued", "Rejected", "Revoked", "Pending"]
        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "month": "Month",
                    "permit_type": "Permit Type",
                    "period": "Period",
                    "area": "Area",
                    "issued": "Issued",
                    "rejected": "Rejected",
                    "revoked": "Revoked",
                    "pending": "Pending",
                }
            )
            for col in target_cols:
                if col not in delta_df.columns:
                    delta_df[col] = pd.NA
            for col in ["id", "Year", "Month"]:
                delta_df[col] = pd.to_numeric(delta_df[col], errors="coerce").astype("Int64")
            delta_df = delta_df[target_cols].sort_values(["Year", "Month", "Permit Type", "Period", "Area"]).reset_index(drop=True)
        else:
            delta_df = pd.DataFrame(columns=target_cols)

        deliverable_name = f"deliverable_{self.pipeline_id}_{datetime.now().strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        write_deliverable_csv(delta_df, deliverable_path)
        new_state = dict(state)
        new_state.update(
            {
                "source_page": self.INDEX_URL,
                "resolved_pdf_url": pdf_url,
                "latest_period_seen": period,
                "file_sha256": file_hash,
                "downloaded_filename": pdf_path.name,
                "last_download_path": str(pdf_path),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
                "table_spec": self.TABLE_SPEC,
                "rows_before": res.rows_before,
                "rows_after": res.rows_after,
                "new_rows": res.new_rows,
                "updated_cells": res.updated_cells,
                "db_comparison": {
                    "status": db_comp_res.get("status"),
                    "missing_in_db": db_comp_res.get("inserted"),
                    "different_in_db": db_comp_res.get("updated"),
                },
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
            return {"status": "skipped", "message": "Source Appendix B PDF unchanged and no data differences detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Downloaded latest Appendix B PDF ({period}) and extracted {len(df_new)} rows. "
                f"DB (athena) comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
