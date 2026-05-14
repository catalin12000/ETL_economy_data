"""
One-shot refactor: update every pipeline.py to use PipelinePaths instead of
hardcoded data/downloads / data/outputs / data/db / data/reports paths.

Run from repo root:
    python scripts/refactor_pipeline_paths.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "etl" / "pipelines"

# ---------------------------------------------------------------------------
# Import line to inject at the top of each pipeline
# ---------------------------------------------------------------------------
IMPORT_LINE = "from etl.core.paths import PipelinePaths\n"

# ---------------------------------------------------------------------------
# Replacement rules — applied in ORDER (later rules can reference
# variables introduced by earlier ones).
# Each entry: (compiled regex, replacement string / callable)
# ---------------------------------------------------------------------------

def _make_rules(pipeline_id: str):
    pid = re.escape(pipeline_id)
    return [
        # ── 1. Inject PipelinePaths after the last `from etl.core.*` import ──
        # (handled separately in _inject_import)

        # ── 2. Insert  pp = PipelinePaths(self.pipeline_id)  at start of run()
        # after `def run(self, state` line — we need the `prefix` block to be
        # replaced FIRST so we can remove it.
        # We handle this via a dedicated pass.

        # ── 3. Replace  prefix = "XX"  declarations (they become unused)
        (
            re.compile(r"^\s+prefix\s*=\s*[\"'][^\"']+[\"']\s*\n", re.MULTILINE),
            "",
        ),

        # ── 4. out_dir / download paths  ──────────────────────────────────────
        # Pattern: Path("data/downloads") / f"XX_{pipeline_id}" or f"cy_XX_{pipeline_id}"
        (
            re.compile(
                r'Path\("data/downloads"\)\s*/\s*f?"[^"]*' + pid + r'"',
                re.IGNORECASE,
            ),
            "pp.downloaded",
        ),
        # Some pipelines use variable: / f"{prefix}_{self.pipeline_id}"
        (
            re.compile(
                r'Path\("data/downloads"\)\s*/\s*f"\{[^}]+\}_\{self\.pipeline_id\}"',
            ),
            "pp.downloaded",
        ),

        # ── 5. output_dir / outputs paths ────────────────────────────────────
        (
            re.compile(
                r'Path\("data/outputs"\)\s*/\s*f?"[^"]*' + pid + r'"',
                re.IGNORECASE,
            ),
            "pp.output",
        ),
        (
            re.compile(
                r'Path\("data/outputs"\)\s*/\s*f"\{[^}]+\}_\{self\.pipeline_id\}"',
            ),
            "pp.output",
        ),

        # ── 6. db_path / baseline ─────────────────────────────────────────────
        # e.g.  Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        (
            re.compile(
                r'Path\("data/db"\)\s*/\s*f?"[^"]*' + pid + r'\.csv"',
                re.IGNORECASE,
            ),
            "pp.baseline",
        ),
        (
            re.compile(
                r'Path\("data/db"\)\s*/\s*f"\{[^}]+\}[^"]*\.csv"',
            ),
            "pp.baseline",
        ),

        # ── 7. report_csv paths ───────────────────────────────────────────────
        (
            re.compile(
                r'Path\("data/reports"\)\s*/\s*f?"[^"]*' + pid + r'"[^/\n]*/\s*"update_report\.csv"',
                re.IGNORECASE,
            ),
            'pp.output / "update_report.csv"',
        ),
        (
            re.compile(
                r'Path\("data/reports"\)\s*/\s*f"\{[^}]+\}[^"]*"\s*/\s*"update_report\.csv"',
            ),
            'pp.output / "update_report.csv"',
        ),

        # ── 8. SQL file path ──────────────────────────────────────────────────
        # Path(__file__).parent / "some_id.sql"
        (
            re.compile(
                r'Path\(__file__\)\.parent\s*/\s*"([^"]+\.sql)"',
            ),
            lambda m: f'pp.sql("{m.group(1)}")',
        ),

        # ── 9. out_dir.mkdir / output_dir.mkdir calls that are now redundant
        #    (PipelinePaths auto-creates dirs).  Remove standalone mkdir lines.
        (
            re.compile(
                r"^\s+(out_dir|output_dir)\.mkdir\([^)]*\)\s*\n",
                re.MULTILINE,
            ),
            "",
        ),
    ]


def _inject_import(code: str) -> str:
    """Insert  from etl.core.paths import PipelinePaths  if not already there."""
    if "PipelinePaths" in code:
        return code
    # Find last `from etl.core.` import line
    lines = code.splitlines(keepends=True)
    last_etl_import = -1
    for i, ln in enumerate(lines):
        if ln.startswith("from etl.core.") or ln.startswith("from etl.pipelines."):
            last_etl_import = i
    if last_etl_import >= 0:
        lines.insert(last_etl_import + 1, IMPORT_LINE)
    else:
        # Fallback: insert after last `import` block
        for i, ln in enumerate(lines):
            if ln.startswith("import ") or ln.startswith("from "):
                last_etl_import = i
        lines.insert(last_etl_import + 1, IMPORT_LINE)
    return "".join(lines)


def _inject_pp(code: str) -> str:
    """
    Insert  pp = PipelinePaths(self.pipeline_id)  as the first line inside
    every  def run(self, state  method body.
    """
    if "pp = PipelinePaths" in code:
        return code

    PP_LINE = "        pp = PipelinePaths(self.pipeline_id)\n"

    def replacer(m: re.Match) -> str:
        # m.group(0) = the def line + first body line
        defline = m.group(0)
        return defline + PP_LINE

    # Match:  def run(self, state...):  followed by newline + optional docstring
    pattern = re.compile(
        r"(    def run\(self[^)]*\)[^:]*:[ \t]*\n)",
        re.MULTILINE,
    )
    return pattern.sub(replacer, code)


def refactor_file(path: Path, dry_run: bool = False) -> bool:
    """Apply all path refactors to one pipeline.py.  Returns True if changed."""
    pipeline_id = path.parent.name
    original = path.read_text(encoding="utf-8")
    code = original

    # Apply regex rules
    for pattern, replacement in _make_rules(pipeline_id):
        if callable(replacement):
            code = pattern.sub(replacement, code)
        else:
            code = pattern.sub(replacement, code)

    # Structural insertions
    code = _inject_import(code)
    code = _inject_pp(code)

    if code == original:
        return False

    if not dry_run:
        path.write_text(code, encoding="utf-8")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    changed, skipped = [], []
    for pdir in sorted(PIPELINES.iterdir()):
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        pf = pdir / "pipeline.py"
        if not pf.exists():
            continue
        if refactor_file(pf, dry_run=args.dry_run):
            changed.append(pdir.name)
            print(f"  {'[DRY]' if args.dry_run else '[OK] '} {pdir.name}")
        else:
            skipped.append(pdir.name)

    print(f"\nChanged: {len(changed)}  Unchanged: {len(skipped)}")
    if args.dry_run:
        print("(dry run — no files written)")


if __name__ == "__main__":
    main()
