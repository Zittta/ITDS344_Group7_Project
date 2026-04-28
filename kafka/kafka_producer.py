"""
Kafka Producer — Seattle Public Safety Streaming Ingestion
==========================================================
Polls 2 Socrata APIs continuously and publishes raw records to Kafka topics:

  bronze_911_calls      ← Seattle Real-Time Fire 911 Calls (kzjm-xkqj)
  bronze_crime_reports  ← SPD Crime Data 2008-Present (tazs-3rd5)

Watermark storage:
  MongoDB  bronze.watermarks  (updateOne / upsert)
  Schema per document:
    {
      _id          : ObjectId,
      layer        : "bronze",
      source       : "seattle_911" | "spd_crime",
      last_ingested_datetime : ISODate,
      rows_loaded  : int,
      status       : "complete" | "running" | "init",
      updated_at   : ISODate,
    }

  On first run the document is initialised with
  last_ingested_datetime = 2024-01-01T00:00:00 so that ALL
  historical data (from that date onward) is fetched — even if
  the Docker volume is brand-new.

Run: docker service (continuous loop, restarts automatically)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("kafka_producer")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SOCRATA_TOKEN   = os.getenv("SOCRATA_APP_TOKEN", "")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB        = "bronze"
WM_COLLECTION   = "watermarks"

# Sentinel: fetch everything from this date when there is no watermark at all
INIT_WATERMARK  = "2024-01-01T00:00:00.000"

SOURCES = [
    {
        "name":                 "seattle_911",
        "topic":                "bronze_911_calls",
        "api_url":              "https://data.seattle.gov/resource/kzjm-xkqj.json",
        "timestamp_field":      "datetime",
        "poll_interval_seconds": 300,   # 5 min — matches dataset refresh rate
    },
    {
        "name":                 "spd_crime",
        "topic":                "bronze_crime_reports",
        "api_url":              "https://data.seattle.gov/resource/tazs-3rd5.json",
        "timestamp_field":      "report_date_time",
        "poll_interval_seconds": 3600,  # 1 hour — crime data updates daily
    },
]

BATCH_SIZE = 50_000   # Socrata max per request


# ─── MongoDB helpers ───────────────────────────────────────────────────────────

def _get_mongo_db():
    """Connect to MongoDB and ensure the watermarks collection is ready."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30_000)
    db = client[MONGO_DB]
    db[WM_COLLECTION].create_index(
        [("layer", 1), ("source", 1)],
        unique=True,
        background=True,
    )
    log.info("Connected to MongoDB: %s / %s", MONGO_URI, MONGO_DB)
    return client, db


# ─── Watermark helpers ─────────────────────────────────────────────────────────

def _init_watermark(db, source_name: str) -> None:
    """
    Insert the watermark document with last_ingested_datetime = INIT_WATERMARK
    if it does not exist yet.  Uses $setOnInsert so re-runs are safe.
    """
    init_dt = datetime.strptime(INIT_WATERMARK[:19], "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    db[WM_COLLECTION].update_one(
        {"layer": "bronze", "source": source_name},
        {
            "$setOnInsert": {
                "layer":                  "bronze",
                "source":                 source_name,
                "last_ingested_datetime": init_dt,
                "rows_loaded":            0,
                "status":                 "init",
                "updated_at":             datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
    log.info("[%s] Watermark initialised (or already existed).", source_name)


def _load_watermark(db, source_name: str) -> str:
    """
    Read the current watermark from MongoDB.
    Returns an ISO-8601 string suitable for the Socrata $where clause.
    Falls back to INIT_WATERMARK if the document has no valid timestamp.
    """
    doc = db[WM_COLLECTION].find_one({"layer": "bronze", "source": source_name})
    if doc:
        ts = doc.get("last_ingested_datetime")
        if isinstance(ts, datetime):
            return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}"
        if isinstance(ts, str) and ts:
            return ts
    log.warning("[%s] No watermark found — using init value %s", source_name, INIT_WATERMARK)
    return INIT_WATERMARK


def _save_watermark(db, source_name: str, dt_str: str, rows_loaded: int) -> None:
    """
    Persist the new watermark to MongoDB using updateOne / upsert.

    Document shape written to bronze.watermarks:
      layer                  : "bronze"
      source                 : source_name
      last_ingested_datetime : ISODate
      rows_loaded            : int   (records published in this batch)
      status                 : "complete"
      updated_at             : ISODate (now)
    """
    try:
        dt = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        log.warning("[%s] Cannot parse watermark timestamp '%s' — skipping save.", source_name, dt_str)
        return

    db[WM_COLLECTION].update_one(
        {"layer": "bronze", "source": source_name},
        {
            "$set": {
                "last_ingested_datetime": dt,
                "rows_loaded":            rows_loaded,
                "status":                 "complete",
                "updated_at":             datetime.now(timezone.utc),
            },
            # Ensure structural fields exist on first upsert
            "$setOnInsert": {
                "layer":  "bronze",
                "source": source_name,
            },
        },
        upsert=True,
    )
    log.info(
        "[%s] Watermark saved → %s  (rows_loaded=%d)",
        source_name, dt_str, rows_loaded,
    )


# ─── Kafka helpers ─────────────────────────────────────────────────────────────

def _create_producer() -> KafkaProducer:
    """Retry connecting to Kafka broker until available."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            log.info("Connected to Kafka broker: %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready yet — retrying in 10s...")
            time.sleep(10)


# ─── Fetch + Publish ───────────────────────────────────────────────────────────

def _fetch_and_publish(producer: KafkaProducer, db, source: dict) -> None:
    """
    Fetch all new records from Socrata API since last watermark
    and publish each record as an individual Kafka message.
    Watermark is saved to MongoDB after a successful publish.
    """
    source_name = source["name"]
    ts_field    = source["timestamp_field"]

    last_dt = _load_watermark(db, source_name)
    log.info("[%s] Fetching since: %s", source_name, last_dt)

    headers    = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    all_records: list[dict] = []
    offset = 0

    while True:
        params = {
            "$where":  f"{ts_field} > '{last_dt}'",
            "$limit":  BATCH_SIZE,
            "$offset": offset,
            "$order":  f"{ts_field} ASC",
        }
        try:
            resp = requests.get(
                source["api_url"], headers=headers, params=params, timeout=120
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("[%s] API request failed: %s", source_name, exc)
            return

        batch: list[dict] = resp.json()
        if not batch:
            break

        all_records.extend(batch)
        log.info(
            "[%s] Page fetched: %d records (total so far: %d)",
            source_name, len(batch), len(all_records),
        )

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    if not all_records:
        log.info("[%s] No new records.", source_name)
        return

    # Publish each record to Kafka topic
    for record in all_records:
        producer.send(source["topic"], value=record)
    producer.flush()

    # Update watermark to the latest timestamp seen
    max_dt = max(r[ts_field] for r in all_records if ts_field in r)
    _save_watermark(db, source_name, max_dt, len(all_records))

    log.info(
        "[%s] Published %d records to topic '%s'. New watermark: %s",
        source_name, len(all_records), source["topic"], max_dt,
    )


# ─── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    mongo_client, db = _get_mongo_db()
    producer = _create_producer()

    # ── Step 1: Init watermarks for ALL sources (safe — $setOnInsert) ──────────
    for source in SOURCES:
        _init_watermark(db, source["name"])

    # ── Step 2: One-shot fetch and publish ─────────────────────────────────────
    for source in SOURCES:
        _fetch_and_publish(producer, db, source)

    log.info("Kafka producer finished one-shot fetch and publish.")
    mongo_client.close()


if __name__ == "__main__":
    main()
