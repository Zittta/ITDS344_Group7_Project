"""
Seattle Real-Time Fire 911 Calls – Data Ingestion DAG
======================================================
Source  : https://data.seattle.gov/resource/kzjm-xkqj.json  (Socrata SODA API)
Schedule: Hourly  (Near Real-time – dataset updates every 5 minutes)
Strategy: Incremental Load – pulls only records newer than the last
          ingested `datetime`.  Re-runs are safe (Idempotent).

Bronze output:
    data/bronze/seattle_911/<YYYY>/<MM>/<DD>/seattle_911_<YYYYmmdd_HHMMSS>.json

State file:
    data/state/seattle_911_state.json
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
API_URL = "https://data.seattle.gov/resource/kzjm-xkqj.json"
BRONZE_DIR = "/opt/airflow/data/bronze/seattle_911"
STATE_FILE = "/opt/airflow/data/state/seattle_911_state.json"

# Max records per API request (Socrata hard limit = 50,000)
BATCH_SIZE = 50_000

# On the very first run, look back this many days
INITIAL_LOOKBACK_DAYS = 30

# ─── State helpers ────────────────────────────────────────────────────────────

def _load_state() -> dict:
    """Return the persisted ingestion state, or a sensible default."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    default_dt = (datetime.utcnow() - timedelta(days=INITIAL_LOOKBACK_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    return {"last_ingested_datetime": default_dt}


def _save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─── Task functions ───────────────────────────────────────────────────────────

def fetch_and_save(**context) -> dict:
    """Fetch new Seattle 911 records and write them to the Bronze layer."""
    state = _load_state()
    last_dt: str = state["last_ingested_datetime"]
    log.info("Fetching Seattle 911 calls since: %s", last_dt)

    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    params = {
        "$where": f"datetime > '{last_dt}'",
        "$limit": BATCH_SIZE,
        "$order": "datetime ASC",
    }

    response = requests.get(API_URL, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    records: list[dict] = response.json()

    if not records:
        log.info("No new records found since %s.", last_dt)
        return {"count": 0, "max_datetime": last_dt}

    log.info("Fetched %d new records.", len(records))

    # Build the Bronze file path: data/bronze/seattle_911/YYYY/MM/DD/
    logical_date: datetime = context["logical_date"]
    run_ts = logical_date.strftime("%Y%m%d_%H%M%S")
    date_dir = os.path.join(BRONZE_DIR, logical_date.strftime("%Y/%m/%d"))
    os.makedirs(date_dir, exist_ok=True)
    filepath = os.path.join(date_dir, f"seattle_911_{run_ts}.json")

    # Idempotency: if the file already exists (re-run), skip writing
    if os.path.exists(filepath):
        log.warning("File already exists – skipping write (idempotent re-run): %s", filepath)
    else:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        log.info("Saved to: %s", filepath)

    max_dt = max(r["datetime"] for r in records if "datetime" in r)
    return {"count": len(records), "max_datetime": max_dt, "filepath": filepath}


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
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="ingestion_seattle_911",
    default_args=default_args,
    description="[Bronze] Incremental ingestion of Seattle Real-Time Fire 911 Calls",
    schedule="@hourly",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["bronze", "ingestion", "seattle", "911"],
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
