"""
SPD Crime Data: 2008-Present – Data Ingestion DAG
==================================================
Source  : https://data.seattle.gov/resource/tazs-3rd5.json  (Socrata SODA API)
Schedule: @daily  (dataset updated every 24 hours by Seattle Police Department)
Strategy: Incremental Load with Pagination – pulls only records where
          `report_date_time` > last ingested timestamp.  Handles large
          batches via offset pagination.  Re-runs are safe (Idempotent).

Key columns:
    report_number, report_date_time, offense_id, offense_date,
    nibrs_group_a_b, nibrs_crime_against_category, offense_sub_category,
    shooting_type_group, block_address, latitude, longitude,
    beat, precinct, sector, neighborhood

Bronze output:
    data/bronze/spd_crime/<YYYY>/<MM>/<DD>/spd_crime_<YYYYmmdd_HHMMSS>.json

State file:
    data/state/spd_crime_state.json
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
SOCRATA_TOKEN: str = os.getenv("SOCRATA_APP_TOKEN", "")
API_URL = "https://data.seattle.gov/resource/tazs-3rd5.json"
BRONZE_DIR = "/opt/airflow/data/bronze/spd_crime"
STATE_FILE = "/opt/airflow/data/state/spd_crime_state.json"

BATCH_SIZE = 50_000          # Records per API page (Socrata max)
INITIAL_START_DATE = "2024-01-01T00:00:00"  # Default watermark on first run

# ─── State helpers ────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_ingested_datetime": INITIAL_START_DATE}


def _save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─── Task functions ───────────────────────────────────────────────────────────

def fetch_and_save(**context) -> dict:
    """
    Fetch all SPD crime records newer than `last_ingested_datetime` using
    offset pagination, then write them as a single JSON file to Bronze.

    Watermark field: `report_date_time`
        The timestamp when the offense was reported to SPD.  Using this field
        (rather than offense_date) ensures we capture all newly approved records
        even if the offense occurred in the past.
    """
    state = _load_state()
    last_dt: str = state["last_ingested_datetime"]
    log.info("Fetching SPD crime records since: %s", last_dt)

    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    all_records: list[dict] = []
    offset = 0

    while True:
        params = {
            "$where": f"report_date_time > '{last_dt}'",
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": "report_date_time ASC",
        }
        response = requests.get(API_URL, headers=headers, params=params, timeout=120)
        response.raise_for_status()
        batch: list[dict] = response.json()

        if not batch:
            break  # No more pages

        all_records.extend(batch)
        log.info(
            "Page fetched: %d records (cumulative: %d)", len(batch), len(all_records)
        )

        if len(batch) < BATCH_SIZE:
            break  # Last (partial) page

        offset += BATCH_SIZE

    if not all_records:
        log.info("No new records found since %s.", last_dt)
        return {"count": 0, "max_datetime": last_dt}

    log.info("Total new records fetched: %d", len(all_records))

    # Write to Bronze
    logical_date: datetime = context["logical_date"]
    run_ts = logical_date.strftime("%Y%m%d_%H%M%S")
    date_dir = os.path.join(BRONZE_DIR, logical_date.strftime("%Y/%m/%d"))
    os.makedirs(date_dir, exist_ok=True)
    filepath = os.path.join(date_dir, f"spd_crime_{run_ts}.json")

    # Idempotency: skip write if file already exists from a previous run
    if os.path.exists(filepath):
        log.warning("File already exists – skipping write (idempotent re-run): %s", filepath)
    else:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False)
        log.info("Saved %d records to: %s", len(all_records), filepath)

    max_dt = max(
        r["report_date_time"] for r in all_records if "report_date_time" in r
    )
    return {"count": len(all_records), "max_datetime": max_dt, "filepath": filepath}


def update_state(**context) -> None:
    """Persist the latest ingested timestamp for the next DAG run."""
    result: dict = context["ti"].xcom_pull(task_ids="fetch_and_save")
    if result and result.get("count", 0) > 0:
        new_state = {
            "last_ingested_datetime": result["max_datetime"],
            "last_run": datetime.utcnow().isoformat(),
            "last_count": result["count"],
        }
        _save_state(new_state)
        log.info("State updated → last_ingested_datetime: %s", result["max_datetime"])
    else:
        log.info("No new data – state unchanged.")


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "group7",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="ingestion_spd_crime",
    default_args=default_args,
    description="[Bronze] Incremental ingestion of SPD Crime Data 2008-Present",
    schedule="@daily",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["bronze", "ingestion", "seattle", "crime", "spd"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_and_save",
        python_callable=fetch_and_save,
    )

    t_update_state = PythonOperator(
        task_id="update_state",
        python_callable=update_state,
    )

    t_fetch >> t_update_state
