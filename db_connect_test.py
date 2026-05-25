"""
Standalone connectivity test against the Postgres DBs defined in .env.

Tests both layers explicitly:
  - psycopg2 raw connection (DBAPI level)
  - SQLAlchemy engine + connection (ORM/query layer used by the pipelines)

Probes each target DB (athena, zeus):
  - server version
  - server time
  - count of tables in public schema
  - row count from one known table

Run with:
    python db_connect_test.py

Returns exit code 0 only if every probe on every DB succeeds.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
TARGETS = ["athena", "zeus"]
SAMPLE_TABLE = {
    "athena": "ed_consumer_price_index",
    "zeus": "ed_construction_index_cy",
}


def load_env(env_path: Path) -> None:
    """Minimal .env loader — only sets keys not already in os.environ."""
    if not env_path.exists():
        raise FileNotFoundError(f"No .env file at {env_path}")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def _url_for(db_name: str) -> sqlalchemy.engine.URL:
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        raise EnvironmentError("DATABASE_URL is not set after loading .env")
    return make_url(raw).set(database=db_name)


def probe_psycopg2(db_name: str) -> dict:
    """Raw psycopg2 — the actual driver."""
    started = time.perf_counter()
    info: dict = {"layer": "psycopg2", "db": db_name}
    url = _url_for(db_name)
    try:
        # psycopg2 wants a plain DSN — render the URL with sslmode preserved.
        dsn = url.render_as_string(hide_password=False)
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version(), now()")
                version, now = cur.fetchone()
                info["version"] = version.splitlines()[0]
                info["server_time"] = str(now)
        info["status"] = "ok"
    except Exception as e:
        info["status"] = "error"
        info["error"] = repr(e)
    info["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
    return info


def probe_sqlalchemy(db_name: str) -> dict:
    """SQLAlchemy engine — the abstraction the pipelines actually use."""
    started = time.perf_counter()
    info: dict = {"layer": "sqlalchemy", "db": db_name}
    url = _url_for(db_name)
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            info["public_table_count"] = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema='public'"
                )
            ).scalar()

            sample = SAMPLE_TABLE.get(db_name)
            if sample is not None:
                try:
                    info["sample_table"] = sample
                    info["sample_row_count"] = conn.execute(
                        text(f'SELECT COUNT(*) FROM "public"."{sample}"')
                    ).scalar()
                except Exception as e:
                    info["sample_table_error"] = str(e)
        info["status"] = "ok"
    except Exception as e:
        info["status"] = "error"
        info["error"] = repr(e)
    info["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
    return info


def main() -> int:
    load_env(ENV_FILE)

    print(f"Project: {ROOT.name}")
    print(f"Reading DATABASE_URL from: {ENV_FILE}")
    print(f"sqlalchemy: {sqlalchemy.__version__}")
    print(f"psycopg2:   {psycopg2.__version__}")
    print()

    exit_code = 0
    for db in TARGETS:
        print(f"=== {db} ===")
        p1 = probe_psycopg2(db)
        if p1["status"] == "ok":
            print(f"  [psycopg2]   OK   ({p1['elapsed_ms']} ms)")
            print(f"    server time: {p1['server_time']}")
            print(f"    version:     {p1['version']}")
        else:
            print(f"  [psycopg2]   FAIL ({p1['elapsed_ms']} ms)")
            print(f"    error: {p1['error']}")
            exit_code = 1

        p2 = probe_sqlalchemy(db)
        if p2["status"] == "ok":
            print(f"  [sqlalchemy] OK   ({p2['elapsed_ms']} ms)")
            print(f"    public tables: {p2['public_table_count']}")
            if "sample_row_count" in p2:
                print(f"    {p2['sample_table']}: {p2['sample_row_count']} rows")
            elif "sample_table_error" in p2:
                print(f"    {p2['sample_table']}: ERROR {p2['sample_table_error']}")
        else:
            print(f"  [sqlalchemy] FAIL ({p2['elapsed_ms']} ms)")
            print(f"    error: {p2['error']}")
            exit_code = 1
        print()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
