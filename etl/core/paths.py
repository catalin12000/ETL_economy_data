"""
Centralised path resolver for all pipeline I/O.

New folder structure
====================
data/
  pipelines/
    <pipeline_id>/
      downloaded/
        <YYYY-MM>/          <- raw source files (one sub-folder per run month)
      output/
        <YYYY-MM>/          <- deliverables, snapshots, reports (one sub-folder per run month)
      scripts/              <- SQL query files for this pipeline
      state.json            <- run state (replaces data/state/<id>.json)
      baseline.csv          <- local comparison baseline (replaces data/db/<prefix>_<id>.csv)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

_DATA_ROOT = Path("data") / "pipelines"


class PipelinePaths:
    """
    All I/O paths for one pipeline, rooted at data/pipelines/<pipeline_id>/.

    Parameters
    ----------
    pipeline_id : str
        The canonical pipeline id, e.g. ``ed_motor_trade_turnover_index``.
    run_date : datetime, optional
        Defaults to ``datetime.now()``.  The month stamp (``YYYY-MM``) derived
        from this date is used to bucket downloaded files and outputs so that
        every run month keeps its own history.

    Usage
    -----
    At the top of ``Pipeline.run()``::

        pp = PipelinePaths(self.pipeline_id)
        xls_path    = pp.downloaded / "elstat_source.xls"
        output_dir  = pp.output
        db_path     = pp.baseline
        sql_path    = pp.sql()
    """

    def __init__(self, pipeline_id: str, run_date: datetime | None = None) -> None:
        self._id = pipeline_id
        self._stamp = (run_date or datetime.now()).strftime("%Y-%m")
        self._root = _DATA_ROOT / pipeline_id

    # ------------------------------------------------------------------ #
    # Public properties                                                    #
    # ------------------------------------------------------------------ #

    @property
    def root(self) -> Path:
        """``data/pipelines/<id>/``"""
        self._root.mkdir(parents=True, exist_ok=True)
        return self._root

    @property
    def downloaded(self) -> Path:
        """``data/pipelines/<id>/downloaded/<YYYY-MM>/``  (auto-created)"""
        p = self._root / "downloaded" / self._stamp
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def output(self) -> Path:
        """``data/pipelines/<id>/output/<YYYY-MM>/``  (auto-created)"""
        p = self._root / "output" / self._stamp
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def scripts(self) -> Path:
        """``data/pipelines/<id>/scripts/``  (auto-created)"""
        p = self._root / "scripts"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def state(self) -> Path:
        """``data/pipelines/<id>/state.json``"""
        return self._root / "state.json"

    @property
    def baseline(self) -> Path:
        """``data/pipelines/<id>/baseline.csv``  — local comparison baseline."""
        return self._root / "baseline.csv"

    def sql(self, name: str | None = None) -> Path:
        """
        Path to the SQL file in the ``scripts/`` folder.

        Parameters
        ----------
        name : str, optional
            Filename without path.  Defaults to ``<pipeline_id>.sql``.
        """
        return self.scripts / (name or f"{self._id}.sql")
