import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

def get_engine(db_name: str | None = None):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL environment variable not set")

    url = make_url(db_url)

    if db_name:
        url = url.set(database=db_name)

    return create_engine(url)

def test_db(db_name: str):
    print(f"\n=== Testing database: {db_name} ===")

    engine = get_engine(db_name)

    with engine.connect() as conn:
        # 1) Prove which DB we are in
        info = conn.execute(
            text("SELECT current_database() AS db, current_schema() AS schema")
        ).mappings().one()

        print(f"Connected to database: {info['db']}")
        print(f"Current schema: {info['schema']}")

        # 2) Verify table exists
        exists = conn.execute(
            text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = 'ed_eu_gdp'
                )
            """)
        ).scalar()

        if not exists:
            print("❌ Table public.ed_eu_gdp NOT FOUND")
            return

        print("✅ Table public.ed_eu_gdp exists")

        # 3) Run a small query
        df = pd.read_sql(
            text("""
                SELECT
                  id,
                  geopolitical_entity,
                  year,
                  quarter
                FROM public.ed_eu_gdp
                ORDER BY year DESC
                LIMIT 5
            """),
            conn
        )

        print("\nSample rows:")
        print(df.to_string(index=False))

def main():
    # Greece
    test_db("athena")

    # Cyprus
    test_db("zeus")

if __name__ == "__main__":
    main()
