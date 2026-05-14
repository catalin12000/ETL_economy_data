from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PIPELINES_ROOT = ROOT / "etl" / "pipelines"
STATE_ROOT = ROOT / "data" / "state"
OUT_PATH = ROOT / "dashboard_codex.md"


IMPLEMENTATION_LABELS = {
    "db_compare": "Fully implemented (extract + local compare + live Postgres diff)",
    "local_compare": "Implemented (extract + local baseline compare only)",
    "data_hash_extract": "Implemented (extract + semantic data hash)",
    "placeholder_dependent": "Placeholder dependent (shared source tracked, extraction pending)",
    "downloader_only": "Downloader/source-only (no extraction or compare yet)",
}

IMPLEMENTATION_ORDER = [
    "db_compare",
    "local_compare",
    "data_hash_extract",
    "placeholder_dependent",
    "downloader_only",
]

PHASE_ORDER = ["good", "blocked_config", "broken", "not_run"]
PHASE_LABELS = {
    "good": "Good",
    "blocked_config": "Blocked (config)",
    "broken": "Broken",
    "not_run": "Not run yet",
}

TECHNICAL_ORDER = ["delivered", "skipped", "error", "never", "state_read_error", "unknown"]

DB_BLOCKED_PATTERNS = [
    "database_url environment variable not set",
    "password authentication failed",
    "could not translate host name",
    "connection refused",
    "timed out",
    "timeout expired",
    "sslmode",
]

SOURCE_FAILURE_PATTERNS = [
    "not found",
    "could not find",
    "404",
    "file not found",
    "filenotfounderror",
    "no yearly pages found",
    "no pdf found",
    "could not find any",
]


@dataclass
class PipelineRow:
    folder: str
    pipeline_id: str
    implementation: str
    technical_status: str
    phase_status: str
    phase_detail: str
    last_run_utc: str
    last_message: str
    notes: str


def _escape_md(value: str) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def _clip(value: str, max_len: int = 120) -> str:
    v = value.strip()
    if len(v) <= max_len:
        return v
    return v[: max_len - 3] + "..."


def _read_state(folder_name: str, declared_pipeline_id: str) -> tuple[str, str, str]:
    for key in (folder_name, declared_pipeline_id):
        state_path = STATE_ROOT / f"{key}.json"
        if not state_path.exists():
            continue
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            return (
                str(data.get("last_status", "unknown") or "unknown"),
                str(data.get("last_run_at_utc", "-") or "-"),
                str(data.get("last_message", "") or ""),
            )
        except Exception:
            return ("state_read_error", "-", "")
    return ("never", "-", "")


def _classification_from_content(content: str) -> str:
    if "Ready to extract" in content:
        return "placeholder_dependent"
    if "compare_with_postgres(" in content:
        return "db_compare"
    if "compare_and_update_csv(" in content or "compare_and_update_excel(" in content:
        return "local_compare"
    if "dataframe_sha256(" in content or "data_sha256" in content:
        return "data_hash_extract"
    return "downloader_only"


def _is_db_blocked_by_config(message: str) -> bool:
    low = message.lower()
    return any(p in low for p in DB_BLOCKED_PATTERNS)


def _looks_like_source_failure(message: str) -> bool:
    low = message.lower()
    return any(p in low for p in SOURCE_FAILURE_PATTERNS)


def _phase_status(implementation: str, technical_status: str, message: str) -> tuple[str, str]:
    if technical_status == "never":
        return ("not_run", "No run recorded in state.")

    if implementation == "db_compare":
        if technical_status in ("delivered", "skipped"):
            return ("good", "Pipeline completed expected DB read-only comparison flow.")
        if technical_status == "error" and _is_db_blocked_by_config(message):
            return ("blocked_config", "Pipeline reached DB step but environment/connection is missing.")
        return ("broken", "Pipeline failed before completing its implemented flow.")

    if technical_status in ("delivered", "skipped"):
        return ("good", "Pipeline completed expected phase flow.")
    return ("broken", "Pipeline failed before completing its expected phase.")


def generate() -> None:
    rows: list[PipelineRow] = []
    missing_pipeline_py: list[str] = []

    for d in sorted(PIPELINES_ROOT.iterdir(), key=lambda p: p.name.lower()):
        if not d.is_dir():
            continue

        pipeline_file = d / "pipeline.py"
        if not pipeline_file.exists():
            missing_pipeline_py.append(d.name)
            continue

        content = pipeline_file.read_text(encoding="utf-8", errors="replace")

        id_match = re.search(r'pipeline_id\s*=\s*"([^"]+)"', content)
        pipeline_id = id_match.group(1) if id_match else d.name

        implementation = _classification_from_content(content)
        technical_status, last_run_utc, last_message = _read_state(d.name, pipeline_id)
        phase_status, phase_detail = _phase_status(implementation, technical_status, last_message)

        note = "-"
        if d.name != pipeline_id:
            note = f"Folder={d.name}, pipeline_id={pipeline_id}"

        rows.append(
            PipelineRow(
                folder=d.name,
                pipeline_id=pipeline_id,
                implementation=implementation,
                technical_status=technical_status,
                phase_status=phase_status,
                phase_detail=phase_detail,
                last_run_utc=last_run_utc,
                last_message=last_message,
                notes=note,
            )
        )

    rows.sort(key=lambda r: (IMPLEMENTATION_ORDER.index(r.implementation), r.pipeline_id.lower()))

    impl_counts = defaultdict(int)
    tech_counts = defaultdict(int)
    phase_counts = defaultdict(int)
    source_failures = []
    blocked_db = []

    for r in rows:
        impl_counts[r.implementation] += 1
        tech_counts[r.technical_status] += 1
        phase_counts[r.phase_status] += 1
        if r.technical_status == "error" and _looks_like_source_failure(r.last_message):
            source_failures.append(r)
        if r.phase_status == "blocked_config":
            blocked_db.append(r)

    lines: list[str] = []
    lines.append("# Codex Pipeline Dashboard")
    lines.append("")
    lines.append(f"Generated at: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %z')}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("- Database policy: **READ-ONLY ONLY** (no writes to Postgres).")
    lines.append(f"- Pipeline directories under `etl/pipelines`: **{len([d for d in PIPELINES_ROOT.iterdir() if d.is_dir()])}**")
    lines.append(f"- Runnable pipeline modules (`pipeline.py` exists): **{len(rows)}**")
    lines.append(f"- Missing `pipeline.py`: **{len(missing_pipeline_py)}**")
    if missing_pipeline_py:
        for m in sorted(missing_pipeline_py, key=str.lower):
            lines.append(f"  - `{m}`")
    lines.append("")
    lines.append("### Phase-Status Snapshot")
    lines.append("")
    lines.append("| Phase Status | Count |")
    lines.append("|---|---:|")
    for key in PHASE_ORDER:
        lines.append(f"| {PHASE_LABELS[key]} | {phase_counts[key]} |")
    lines.append("")
    lines.append("### Technical-Status Snapshot")
    lines.append("")
    lines.append("| Technical Status | Count |")
    lines.append("|---|---:|")
    for key in TECHNICAL_ORDER:
        if key in tech_counts:
            lines.append(f"| {key} | {tech_counts[key]} |")
    lines.append("")
    lines.append("### Implementation Breakdown")
    lines.append("")
    lines.append("| Implementation Status | Count |")
    lines.append("|---|---:|")
    for key in IMPLEMENTATION_ORDER:
        lines.append(f"| {IMPLEMENTATION_LABELS[key]} | {impl_counts[key]} |")
    lines.append("")
    lines.append("### Error Context")
    lines.append("")
    lines.append(f"- Source/link/file-not-found errors: **{len(source_failures)}**")
    lines.append(f"- DB-config-blocked pipelines: **{len(blocked_db)}**")
    lines.append("")
    lines.append("## Pipeline Status Table")
    lines.append("")
    lines.append("| Pipeline ID | Folder | Implementation Status | Technical Status | Phase Status | Phase Detail | Last Run (UTC) | Last Message | Notes |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        lines.append(
            "| {pid} | {folder} | {impl} | {tech} | {phase} | {detail} | {run} | {msg} | {notes} |".format(
                pid=_escape_md(r.pipeline_id),
                folder=_escape_md(r.folder),
                impl=_escape_md(IMPLEMENTATION_LABELS[r.implementation]),
                tech=_escape_md(r.technical_status),
                phase=_escape_md(PHASE_LABELS[r.phase_status]),
                detail=_escape_md(_clip(r.phase_detail, 90)),
                run=_escape_md(r.last_run_utc),
                msg=_escape_md(_clip(r.last_message, 110) if r.last_message else "-"),
                notes=_escape_md(r.notes),
            )
        )

    OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    generate()
