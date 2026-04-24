"""
Bronze DAG — API Batch Ingestion  (Socrata → MongoDB bronze)
=============================================================
Airflow-controlled BATCH path for 911 calls and SPD crime data.

  seattle_911  → bronze.seattle_911   (watermark: bronze.watermarks)
  spd_crime    → bronze.spd_crime     (watermark: bronze.watermarks)

Incremental Load:
  - Watermark (last_ingested_dt) stored per source in bronze.watermarks.
  - Every run fetches ONLY records newer than the watermark.
  - After successful ingest, watermark advances to the max timestamp seen.

Idempotency:
  - MongoDB upsert with $setOnInsert on the business key ensures that
    re-running the DAG (or overlapping runs) never creates duplicate docs.

Batch processing:
  - Socrata pages are fetched 50 000 rows at a time, upserted in chunks of
    2 000 documents to avoid memory spikes.

Note: This is the BATCH orchestration path.
      The STREAMING bonus path runs continuously via the kafka-producer and
      kafka-consumer-bronze Docker services, both writing to the same
      MongoDB collections (upsert guarantees no conflicts).

Schedule:
  - 911 calls: every 5 min  (both tasks share one DAG scheduled every 5 min;
  - SPD crime: task internally skips if < 60 min since last run)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MONGO_URI      = os.getenv("MONGO_URI", "mongodb://mongo:27017")
SOCRATA_TOKEN  = os.getenv("SOCRATA_APP_TOKEN", "")
BRONZE_DB      = "bronze"
BATCH_SIZE     = 50_000
UPSERT_CHUNK   = 2_000

SOURCES = {
    "seattle_911": {
        "api_url":               "https://data.seattle.gov/resource/kzjm-xkqj.json",
        "timestamp_field":       "datetime",
        "unique_key":            "incident_number",
        "required_fields":       ["incident_number", "datetime"],
        "initial_start_date":    "2024-01-01T00:00:00",
        "min_poll_interval_min": 5,
    },
    "spd_crime": {
        "api_url":               "https://data.seattle.gov/resource/tazs-3rd5.json",
        "timestamp_field":       "report_date_time",
        "unique_key":            "offense_id",
        "required_fields":       ["offense_id", "report_date_time"],
        "initial_start_date":    "2024-01-01T00:00:00",
        "min_poll_interval_min": 60,
    },
}


# ─── Watermark helpers ────────────────────────────────────────────────────────

def _get_watermark(db, source_name: str, default: str) -> str:
    doc = db["watermarks"].find_one({"source": source_name, "layer": "bronze"})
    if doc and doc.get("last_ingested_dt"):
        ts = doc["last_ingested_dt"]
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%dT%H:%M:%S")
    return default


def _set_watermark(db, source_name: str, dt_str: str) -> None:
    try:
        dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return
    db["watermarks"].update_one(
        {"source": source_name, "layer": "bronze"},
        {"$set": {"last_ingested_dt": dt, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def _should_run(db, source_name: str, min_interval_min: int) -> bool:
    """Rate-limit check: skip task if called too soon after last run."""
    doc = db["watermarks"].find_one({"source": source_name, "layer": "bronze"})
    if not doc or "updated_at" not in doc:
        return True
    updated = doc["updated_at"]
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    elapsed_min = (datetime.now(timezone.utc) - updated).total_seconds() / 60
    return elapsed_min >= min_interval_min


# ─── DQ helpers ───────────────────────────────────────────────────────────────

def _validate_records(records: list[dict], cfg: dict) -> tuple[list[dict], int]:
    """
    DQ Rule 1: required fields must be non-empty.
    DQ Rule 2: timestamp field must be non-empty.
    Returns (valid_records, skipped_count).
    """
    valid, skipped = [], 0
    for rec in records:
        if any(not rec.get(f) for f in cfg["required_fields"]):
            skipped += 1
            continue
        if not rec.get(cfg["timestamp_field"]):
            skipped += 1
            continue
        valid.append(rec)
    return valid, skipped


# ─── Core ingest task ─────────────────────────────────────────────────────────

def ingest_source(source_name: str, **context) -> dict:
    cfg    = SOURCES[source_name]
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db     = client[BRONZE_DB]

    # Ensure index on unique key
    db[source_name].create_index(cfg["unique_key"], background=True)

    # Rate-limit: skip if called too soon
    if not _should_run(db, source_name, cfg["min_poll_interval_min"]):
        log.info("[%s] Skipped — ran < %d min ago", source_name, cfg["min_poll_interval_min"])
        client.close()
        return {"skipped": True}

    watermark = _get_watermark(db, source_name, cfg["initial_start_date"])
    ts_field  = cfg["timestamp_field"]
    log.info("[%s] Fetching records since %s", source_name, watermark)

    headers    = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    all_records: list[dict] = []
    offset     = 0

    # ── Paginate Socrata API ──────────────────────────────────────────────────
    while True:
        params = {
            "$where":  f"{ts_field} > '{watermark}'",
            "$limit":  BATCH_SIZE,
            "$offset": offset,
            "$order":  f"{ts_field} ASC",
        }
        try:
            resp = requests.get(cfg["api_url"], headers=headers, params=params, timeout=120)
            resp.raise_for_status()
            batch: list[dict] = resp.json()
        except requests.RequestException as exc:
            log.error("[%s] API error: %s", source_name, exc)
            client.close()
            raise

        if not batch:
            break

        all_records.extend(batch)
        log.info("[%s] page=%d total_so_far=%d", source_name, len(batch), len(all_records))

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    if not all_records:
        log.info("[%s] No new records", source_name)
        client.close()
        return {"fetched": 0, "skipped_dq": 0}

    # ── DQ validation ─────────────────────────────────────────────────────────
    valid_records, skipped_dq = _validate_records(all_records, cfg)
    log.info("[%s] DQ: total=%d valid=%d skipped=%d", source_name,
             len(all_records), len(valid_records), skipped_dq)

    # ── Upsert to MongoDB in chunks ───────────────────────────────────────────
    collection = db[source_name]
    now        = datetime.now(timezone.utc)
    total_upserted = 0

    for i in range(0, len(valid_records), UPSERT_CHUNK):
        chunk = valid_records[i:i + UPSERT_CHUNK]
        ops   = []
        for rec in chunk:
            doc = {**rec, "_source": source_name, "_ingested_at": now}
            ops.append(UpdateOne(
                {cfg["unique_key"]: rec[cfg["unique_key"]]},
                {
                    "$setOnInsert": doc,                       # idempotent: only on first insert
                    "$set":         {"_last_seen_at": now},
                },
                upsert=True,
            ))
        try:
            result = collection.bulk_write(ops, ordered=False)
            total_upserted += result.upserted_count
        except BulkWriteError as exc:
            log.warning("[%s] BulkWriteError (non-fatal): %d errors",
                        source_name, len(exc.details.get("writeErrors", [])))

    # ── Advance watermark ─────────────────────────────────────────────────────
    max_dt = max(r[ts_field] for r in valid_records if r.get(ts_field))
    _set_watermark(db, source_name, max_dt)

    log.info("[%s] Done: fetched=%d upserted=%d skipped_dq=%d new_watermark=%s",
             source_name, len(all_records), total_upserted, skipped_dq, max_dt)
    client.close()
    return {"fetched": len(all_records), "upserted": total_upserted, "skipped_dq": skipped_dq}


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":            "group7",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=2),
}

with DAG(
    dag_id="bronze_api_ingest",
    description="Batch ingest 911 calls + crime reports from Socrata API → MongoDB bronze",
    default_args=default_args,
    schedule_interval="*/5 * * * *",   # every 5 min (crime task self-throttles to 60 min)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["bronze", "ingestion", "batch", "incremental"],
    max_active_runs=1,
) as dag:

    ingest_911 = PythonOperator(
        task_id="ingest_911_calls",
        python_callable=ingest_source,
        op_kwargs={"source_name": "seattle_911"},
        doc_md="Fetch new 911 call records from Socrata API and upsert into bronze.seattle_911",
    )

    ingest_crime = PythonOperator(
        task_id="ingest_crime_reports",
        python_callable=ingest_source,
        op_kwargs={"source_name": "spd_crime"},
        doc_md="Fetch new crime records from Socrata API and upsert into bronze.spd_crime",
    )

    # Both tasks are independent (different data sources) — run in parallel
    [ingest_911, ingest_crime]
