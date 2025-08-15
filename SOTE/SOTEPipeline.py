from __future__ import annotations
import logging
from datetime import timedelta
from pathlib import Path
import pendulum
import pandas as pd

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Config (Variables -> Admin > Variables)
BASE_DIR = Path(Variable.get("sote_base_path", default_var="/opt/airflow/data/sote"))
RAW_DIR = BASE_DIR / "raw"
STAGE_DIR = BASE_DIR / "stage"

DEFAULT_ARGS = {
    "owner": "kevin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="sote_etl_pipeline",
    description="Ingest SOTE CSV, clean it, and load to Postgres for analysis",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 8, 1, tz="America/Los_Angeles"),
    schedule="0 3 * * *",  # every day at 03:00
    catchup=False,
    max_active_runs=1,
    tags=["sote", "etl", "pandas"],
)
def sote_etl():
    @task
    def queue_data(ds: str) -> str:
        """
        Locate the raw CSV for the partition date and copy to a staging path.
        Returns the staged file path (string).
        """
        raw_path = RAW_DIR / f"SOTE_{ds}.csv"  # e.g., /opt/airflow/data/sote/raw/SOTE_2025-08-15.csv
        if not raw_path.exists():
            raise FileNotFoundError(f"Expected raw file not found: {raw_path}")

        staged_path = STAGE_DIR / f"staged_{ds}.csv"
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.write_bytes(raw_path.read_bytes())
        logging.info("Staged raw CSV: %s", staged_path)
        return str(staged_path)

    @task
    def clean_data(staged_csv_path: str) -> str:
        """
        Read staged CSV, apply cleaning rules, write clean Parquet.
        Returns the clean parquet path (string).
        """
        df = pd.read_csv(staged_csv_path)

        # === EXAMPLES of cleaning steps (customize to your SOTE schema) ===
        # Strip whitespace from column names
        df.columns = [c.strip() for c in df.columns]

        # Drop fully empty columns
        df = df.dropna(axis=1, how="all")

        # Example: standardize grade labels
        if "Official Grade" in df.columns:
            df["Official Grade"] = df["Official Grade"].astype(str).str.upper().str.strip()

        # Example: date parsing
        for col in ("SurveyDate", "Term"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Write out a partitioned artifact
        clean_path = Path(staged_csv_path).with_suffix(".parquet").as_posix().replace("staged_", "clean_")
        pd.DataFrame(df).to_parquet(clean_path, index=False)
        logging.info("Wrote cleaned parquet: %s", clean_path)
        return clean_path

    @task
    def load_to_db(clean_parquet_path: str, table_name: str = "sote_clean") -> str:
        """
        Load cleaned data to Postgres using a connection (airflow conn id: 'analytics_pg').
        Idempotent: overwrites the ds partition if such column exists; else truncates+load (simple demo).
        """
        hook = PostgresHook(postgres_conn_id="analytics_pg")

        df = pd.read_parquet(clean_parquet_path)

        # OPTIONAL: add a partition column based on file name
        # (assumes path like clean_YYYY-MM-DD.parquet)
        p = Path(clean_parquet_path)
        ds_part = p.stem.replace("clean_", "")
        if "ds" not in df.columns:
            df["ds"] = ds_part

        # Create table if needed (very simple schema inference)
        # For production, define DDL explicitly and enforce types.
        engine = hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            # Idempotent load strategy:
            # 1) delete existing partition
            conn.execute(f'DELETE FROM {table_name} WHERE ds = %(ds)s', {"ds": ds_part})
            # 2) append fresh data
            df.to_sql(table_name, con=conn, if_exists="append", index=False)

        logging.info("Loaded %d rows into %s for ds=%s", len(df), table_name, ds_part)
        return f"{table_name}:{ds_part}"

    staged = queue_data()  # ds auto-injected by TaskFlow
    cleaned = clean_data(staged)
    _loaded = load_to_db(cleaned)

dag = sote_etl()