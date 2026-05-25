from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine
from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint
from etl.core.output import write_deliverable_csv


def _parse_period(period: str | None) -> tuple[int | None, int | None]:
    if not period or "-" not in period:
        return None, None
    year, month = period.split("-", 1)
    try:
        return int(year), int(month)
    except ValueError:
        return None, None


def _format_delta(
    delta_df: pd.DataFrame,
    *,
    db_to_target: dict[str, str],
    target_cols: list[str],
    sort_cols: list[str],
) -> pd.DataFrame:
    if delta_df.empty:
        return pd.DataFrame(columns=target_cols)

    formatted = delta_df.rename(columns={"id": "id", **db_to_target}).copy()
    for col in target_cols:
        if col not in formatted.columns:
            formatted[col] = pd.NA

    for col in ["id", "Year", "Month", "Quarter", "Rank"]:
        if col in formatted.columns:
            formatted[col] = pd.to_numeric(formatted[col], errors="coerce").astype("Int64")

    formatted = formatted[target_cols]
    formatted = formatted.sort_values(sort_cols).reset_index(drop=True)
    return formatted


def _period_key(year: int | None, month: int | None) -> int:
    if year is None or month is None:
        return -1
    return year * 100 + month


def _parse_archive_period_from_name(path: Path) -> tuple[int | None, int | None]:
    parts = path.stem.rsplit("_", 2)
    if len(parts) < 3:
        return None, None
    try:
        return int(parts[-2]), int(parts[-1])
    except ValueError:
        return None, None


def _latest_db_period(table_name: str, db_name: str) -> tuple[int | None, int | None]:
    engine = get_engine(db_name)
    query = f'SELECT MAX(year * 100 + month) AS period_key FROM "public"."{table_name}"'
    result = pd.read_sql(query, engine)
    value = result.iloc[0, 0]
    if pd.isna(value):
        return None, None
    value = int(value)
    return value // 100, value % 100


def _existing_db_periods(table_name: str, db_name: str, sync_cols: list[str] | None = None) -> set[int]:
    """
    Return period keys (year*100+month) that are fully populated in the DB.
    A period is considered missing if it doesn't exist OR if any sync_col is NULL.
    """
    try:
        engine = get_engine(db_name)
        if sync_cols:
            not_null = " AND ".join(f'"{c}" IS NOT NULL' for c in sync_cols)
            query = (
                f'SELECT DISTINCT year * 100 + month AS pk FROM "public"."{table_name}" '
                f'WHERE {not_null}'
            )
        else:
            query = f'SELECT DISTINCT year * 100 + month AS pk FROM "public"."{table_name}"'
        result = pd.read_sql(query, engine)
        return set(result["pk"].dropna().astype(int).tolist())
    except Exception:
        return set()


def _collect_snapshot_frames(
    *,
    pdf_path: Path,
    src_period: str | None,
    table_name: str,
    db_name: str,
    extractor: Callable[..., pd.DataFrame],
    sync_cols: list[str] | None = None,
) -> pd.DataFrame:
    existing_keys = _existing_db_periods(table_name, db_name, sync_cols=sync_cols)

    source_candidates: list[tuple[int, int, Path]] = []
    current_year, current_month = _parse_period(src_period)
    current_key = _period_key(current_year, current_month)
    if current_key > 0 and current_key not in existing_keys:
        source_candidates.append((current_year, current_month, pdf_path))

    archive_dir = pdf_path.parent / "archive"
    if archive_dir.exists():
        for archived_pdf in archive_dir.glob("migration_appendix_b_*.pdf"):
            year, month = _parse_archive_period_from_name(archived_pdf)
            key = _period_key(year, month)
            if key > 0 and key not in existing_keys:
                source_candidates.append((year, month, archived_pdf))

    if not source_candidates:
        return extractor(pdf_path, report_year=current_year, report_month=current_month)

    frames: list[pd.DataFrame] = []
    seen_periods: set[tuple[int, int]] = set()
    for year, month, source_pdf in sorted(source_candidates, key=lambda item: (item[0], item[1])):
        period = (year, month)
        if period in seen_periods:
            continue
        seen_periods.add(period)
        try:
            frame = extractor(source_pdf, report_year=year, report_month=month)
        except Exception:
            # Older PDF formats may not be parseable — skip rather than abort.
            continue
        if frame is not None and not frame.empty:
            frames.append(frame)

    # If every candidate produced an empty frame (e.g. all archives are an
    # incompatible older format), fall back to the current PDF so downstream
    # steps still have something to work with.
    if not frames:
        return extractor(pdf_path, report_year=current_year, report_month=current_month)

    return pd.concat(frames, ignore_index=True)


def run_shared_appendix_b_pipeline(
    *,
    state: dict[str, Any],
    prefix: str,
    pipeline_id: str,
    table_spec: str,
    extractor: Callable[..., pd.DataFrame],
    key_cols: list[str],
    target_to_db: dict[str, str],
    match_cols: list[str],
    sync_cols: list[str],
    sql_file_path: str,
    target_cols: list[str],
    source_pipeline_id: str = "ed_geo_distribution_of_issued_and_pending_permits",
    db_name: str = "athena",
    tolerance: float = 0.11,
    snapshot_backfill: bool = False,
) -> dict[str, Any]:
    pdf_path = get_latest_pdf_path(source_pipeline_id)
    src_hash, src_period = get_source_fingerprint(source_pipeline_id)
    report_year, report_month = _parse_period(src_period)

    if snapshot_backfill:
        df_new = _collect_snapshot_frames(
            pdf_path=pdf_path,
            src_period=src_period,
            table_name=pipeline_id,
            db_name=db_name,
            extractor=extractor,
            sync_cols=list(sync_cols),
        )
    else:
        df_new = extractor(pdf_path, report_year=report_year, report_month=report_month)

    from etl.core.paths import PipelinePaths
    pp = PipelinePaths(pipeline_id)
    output_dir = pp.output
    out_csv_full = output_dir / "mock_db_snapshot.csv"
    output_file = output_dir / "new_entries.csv"
    report_csv = output_dir / "update_report.csv"
    db_path = pp.baseline

    res = compare_and_update_csv(
        db_csv_path=db_path,
        extracted_df=df_new,
        out_csv_path=out_csv_full,
        report_csv_path=report_csv,
        key_cols=key_cols,
    )
    res.updated_df.to_csv(out_csv_full, index=False)
    res.diff_df.to_csv(output_file, index=False)

    df_for_db = df_new.rename(columns=target_to_db)
    db_comp_res = compare_with_postgres(
        df=df_for_db,
        table_name=pipeline_id,
        db_name=db_name,
        match_cols=match_cols,
        sync_cols=sync_cols,
        tolerance=tolerance,
        sql_file_path=sql_file_path,
    )
    if db_comp_res.get("error"):
        return {"status": "error", "message": db_comp_res["error"], "state": dict(state)}

    now = datetime.now()
    deliverable_name = f"deliverable_{pipeline_id}_{now.strftime('%B_%Y')}.csv"
    deliverable_path = output_dir / deliverable_name

    inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
    updated_df = db_comp_res.get("updated_df", pd.DataFrame())
    delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
    db_to_target = {db_name: target_name for target_name, db_name in target_to_db.items()}
    formatted_delta = _format_delta(delta_df, db_to_target=db_to_target, target_cols=target_cols, sort_cols=key_cols)
    write_deliverable_csv(formatted_delta, deliverable_path)

    new_state = dict(state)
    new_state.update(
        {
            "source_pipeline_id": source_pipeline_id,
            "source_file_sha256": src_hash,
            "source_latest_period_seen": src_period,
            "source_pdf_path": str(pdf_path),
            "table_spec": table_spec,
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
        not is_new_by_hash(state.get("source_file_sha256"), src_hash)
        and res.new_rows == 0
        and res.updated_cells == 0
        and db_comp_res.get("inserted") == 0
        and db_comp_res.get("updated") == 0
    ):
        return {"status": "skipped", "message": "Source Appendix B PDF unchanged and no data differences detected.", "state": new_state}

    return {
        "status": "delivered",
        "message": (
            f"Extracted {len(df_new)} rows from {table_spec}. "
            f"DB ({db_name}) comparison: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} diff. File: {deliverable_name}"
        ),
        "state": new_state,
    }
