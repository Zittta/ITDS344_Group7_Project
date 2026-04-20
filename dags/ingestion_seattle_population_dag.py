"""
Seattle Neighborhoods ACS Population – Data Ingestion DAG (CSV File Load)
==========================================================================
Source  : "Seattle Neighborhoods - Top 50 American Community Survey Data"
          Dataset ID : 3nzs-xvkv  (data.seattle.gov)
          Download   : https://data.seattle.gov/d/3nzs-xvkv → Export → CSV
          Place file : data/raw_csv/seattle_neighborhoods_acs.csv

Schedule: @yearly  (ACS 5-Year estimates published annually)
Strategy: File-based Full Load – validates then copies the raw CSV as-is
          into the Bronze layer (preserving the original source format).
          Idempotent: skips re-copy if Bronze file already exists for the run.

Why this dataset:
    - Neighborhood geography (Community Reporting Areas) maps directly to
      the `neighborhood` field in SPD Crime Data (tazs-3rd5), enabling
      per-capita crime rate calculations by neighborhood.
    - Contains 50 ACS variables: total population, race/ethnicity, age,
      median household income, poverty rate, housing tenure, etc.
    - Updated annually (ACS 2024 vintage as of March 2026).

Bronze output:
    data/bronze/seattle_population/<YEAR>/seattle_neighborhoods_acs_<YEAR>.csv

State file:
    data/state/seattle_population_state.json
"""
from __future__ import annotations

import csv
import json
import logging
import os
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
# Path to the manually downloaded CSV file (mounted into the container)
CSV_INPUT_PATH = "/opt/airflow/data/raw_csv/seattle_neighborhoods_acs.csv"

BRONZE_DIR = "/opt/airflow/data/bronze/seattle_population"
STATE_FILE = "/opt/airflow/data/state/seattle_population_state.json"

# ACS vintage year of the downloaded file – update this when you download a new file
ACS_YEAR = 2024

# Columns that MUST exist in the CSV (fail fast if missing)
# Names match the human-readable headers in data.seattle.gov/d/3nzs-xvkv export
REQUIRED_COLUMNS = {
    "Neighborhood Name",   # Community Reporting Area name (maps to SPD crime `neighborhood`)
    "Total Population",    # Total population estimate
}


# ─── Task functions ───────────────────────────────────────────────────────────

def validate_csv(**context) -> dict:
    """
    Validate that the CSV file exists, is non-empty, and contains the
    expected required columns before any data is written to Bronze.
    """
    if not os.path.exists(CSV_INPUT_PATH):
        raise FileNotFoundError(
            f"Population CSV not found: {CSV_INPUT_PATH}\n"
            "Please download from:\n"
            "  https://data.seattle.gov/d/3nzs-xvkv → Export → CSV\n"
            f"and save to: {CSV_INPUT_PATH}"
        )

    with open(CSV_INPUT_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV file is empty: {CSV_INPUT_PATH}")

    missing = REQUIRED_COLUMNS - headers
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}\n"
            f"Available columns: {sorted(headers)}"
        )

    log.info(
        "CSV validation passed: %d rows, %d columns detected.",
        len(rows),
        len(headers),
    )
    return {"row_count": len(rows), "column_count": len(headers)}


def load_csv_to_bronze(**context) -> dict:
    """
    Copy the validated CSV as-is into the Bronze layer.
    Preserves the original source format (immutable raw data principle).
    """
    logical_date: datetime = context["logical_date"]

    # Count rows for metadata (don't transform the data)
    with open(CSV_INPUT_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Build Bronze file path – keep .csv extension (same as source)
    year_dir = os.path.join(BRONZE_DIR, str(ACS_YEAR))
    os.makedirs(year_dir, exist_ok=True)
    filepath = os.path.join(
        year_dir, f"seattle_neighborhoods_acs_{ACS_YEAR}.csv"
    )

    # Idempotency: skip if file already exists
    if os.path.exists(filepath):
        log.warning(
            "File already exists – skipping copy (idempotent re-run): %s", filepath
        )
        return {"count": len(rows), "filepath": filepath, "skipped": True}

    # Copy raw CSV as-is (no transformation at Bronze layer)
    shutil.copy2(CSV_INPUT_PATH, filepath)
    log.info("Copied raw CSV (%d rows) to Bronze: %s", len(rows), filepath)
    return {"count": len(rows), "filepath": filepath, "skipped": False}


def update_state(**context) -> None:
    """Persist load metadata for auditing and lineage tracking."""
    result: dict = context["ti"].xcom_pull(task_ids="load_csv_to_bronze")
    validate_result: dict = context["ti"].xcom_pull(task_ids="validate_csv")

    state = {
        "acs_year": ACS_YEAR,
        "last_run": datetime.utcnow().isoformat(),
        "row_count": result.get("count", 0),
        "column_count": validate_result.get("column_count", 0),
        "filepath": result.get("filepath"),
        "skipped": result.get("skipped", False),
    }
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    log.info("State updated – ACS year: %d, rows: %d", ACS_YEAR, state["row_count"])


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "group7",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ingestion_seattle_population",
    default_args=default_args,
    description="[Bronze] File-based load of Seattle Neighborhoods ACS Population Data (CSV)",
    schedule="@yearly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion", "seattle", "population", "acs", "csv"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
    )

    t_load = PythonOperator(
        task_id="load_csv_to_bronze",
        python_callable=load_csv_to_bronze,
    )

    t_state = PythonOperator(
        task_id="update_state",
        python_callable=update_state,
    )

    t_validate >> t_load >> t_state
