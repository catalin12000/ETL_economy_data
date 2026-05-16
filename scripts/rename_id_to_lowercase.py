"""
Rename the deliverable column literal "ID" to "id" across the codebase.

Why: deliverable headers are now fully lowercase snake_case (see
etl/core/output.py::write_deliverable_csv). The pipeline-side
target_cols / rename maps / dataframe indexers used "ID" as a special-case
uppercase column; this script normalises them so the source matches the
output and there's no remaining mismatch.

Replaces only the literal string forms `"ID"` and `'ID'` inside Python files
under etl/, leaving non-column occurrences (none observed) untouched.
"""
from __future__ import annotations

from pathlib import Path

ROOTS = [Path("etl/pipelines"), Path("etl/core")]
PAIRS = [('"ID"', '"id"'), ("'ID'", "'id'")]

changed = 0
for root in ROOTS:
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        new = text
        for old, repl in PAIRS:
            new = new.replace(old, repl)
        if new != text:
            path.write_text(new, encoding="utf-8")
            changed += 1
            print(f"updated: {path}")

print(f"\nTotal files changed: {changed}")
