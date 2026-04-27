"""
Kafka Producer — Seattle Public Safety Streaming Ingestion
==========================================================
Polls 2 Socrata APIs continuously and publishes raw records to Kafka topics:

  bronze_911_calls      ← Seattle Real-Time Fire 911 Calls (kzjm-xkqj)
  bronze_crime_reports  ← SPD Crime Data 2008-Present (tazs-3rd5)

State files (watermark):
  /data/state/kafka_911_state.json
  /data/state/kafka_crime_state.json

Run: docker service (continuous loop, restarts automatically)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("kafka_producer")

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SOCRATA_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")

SOURCES = [
    {
        "name": "seattle_911",
        "topic": "bronze_911_calls",
        "api_url": "https://data.seattle.gov/resource/kzjm-xkqj.json",
        "timestamp_field": "datetime",
        "initial_lookback_days": 30,
        "poll_interval_seconds": 300,   # 5 minutes — matches dataset refresh rate
        "state_file": "/data/state/kafka_911_state.json",
    },
    {
        "name": "spd_crime",
        "topic": "bronze_crime_reports",
        "api_url": "https://data.seattle.gov/resource/tazs-3rd5.json",
        "timestamp_field": "report_date_time",
        "initial_lookback_days": 0,     # start from 2024-01-01
        "initial_start_date": "2024-01-01T00:00:00",
        "poll_interval_seconds": 3600,  # 1 hour — crime data updates daily
        "state_file": "/data/state/kafka_crime_state.json",
    },
]

BATCH_SIZE = 50_000   # Socrata max per request


# ─── State helpers ─────────────────────────────────────────────────────────────

def _load_watermark(source: dict) -> str:
    state_file = source["state_file"]
    if os.path.exists(state_file):
        with open(state_file) as f:
            return json.load(f)["last_ingested_datetime"]
    if "initial_start_date" in source:
        return source["initial_start_date"]
    return (
        datetime.utcnow() - timedelta(days=source["initial_lookback_days"])
    ).strftime("%Y-%m-%dT%H:%M:%S")


def _save_watermark(source: dict, timestamp: str) -> None:
    os.makedirs(os.path.dirname(source["state_file"]), exist_ok=True)
    with open(source["state_file"], "w") as f:
        json.dump({"last_ingested_datetime": timestamp}, f, indent=2)


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

def _fetch_and_publish(producer: KafkaProducer, source: dict) -> None:
    """
    Fetch all new records from Socrata API since last watermark
    and publish each record as an individual Kafka message.
    """
    last_dt = _load_watermark(source)
    ts_field = source["timestamp_field"]
    log.info("[%s] Fetching since: %s", source["name"], last_dt)

    headers = {"X-App-Token": SOCRATA_TOKEN} if SOCRATA_TOKEN else {}
    all_records: list[dict] = []
    offset = 0

    while True:
        params = {
            "$where": f"{ts_field} > '{last_dt}'",
            "$limit": BATCH_SIZE,
            "$offset": offset,
            "$order": f"{ts_field} ASC",
        }
        try:
            resp = requests.get(
                source["api_url"], headers=headers, params=params, timeout=120
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("[%s] API request failed: %s", source["name"], exc)
            return

        batch: list[dict] = resp.json()
        if not batch:
            break

        all_records.extend(batch)
        log.info(
            "[%s] Page fetched: %d records (total so far: %d)",
            source["name"], len(batch), len(all_records),
        )

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    if not all_records:
        log.info("[%s] No new records.", source["name"])
        return

    # Publish each record to Kafka topic
    for record in all_records:
        producer.send(source["topic"], value=record)
    producer.flush()

    # Update watermark to the latest timestamp seen
    max_dt = max(r[ts_field] for r in all_records if ts_field in r)
    _save_watermark(source, max_dt)

    log.info(
        "[%s] Published %d records to topic '%s'. New watermark: %s",
        source["name"], len(all_records), source["topic"], max_dt,
    )


# ─── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    producer = _create_producer()
    for source in SOURCES:
        _fetch_and_publish(producer, source)
    log.info("Kafka producer finished one-shot fetch and publish.")


if __name__ == "__main__":
    main()
