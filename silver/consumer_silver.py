# TODO: Silver Layer — Kafka Consumer + Transform
# ==========================================================
# File: silver/consumer_silver.py
# Purpose:
#   Consume Kafka topics -> Transform -> Write Silver CSV
#
# Topics:
#   bronze_911_calls
#   bronze_crime_reports
#
# Output:
#   /data/silver/silver_911_calls.csv
#   /data/silver/silver_crime_reports.csv
#
# Run:
#   python consumer_silver.py
# ==========================================================

import os
import json
import time
import logging
import pandas as pd

from kafka import KafkaConsumer

# ==========================================================
# CONFIG
# ==========================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPICS = [
    "bronze_911_calls",
    "bronze_crime_reports"
]

SILVER_DIR = "/data/silver"
LOG_DIR = "/logs"

os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

SILVER_911 = f"{SILVER_DIR}/silver_911_calls.csv"
SILVER_CRIME = f"{SILVER_DIR}/silver_crime_reports.csv"

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    filename=f"{LOG_DIR}/consumer_silver.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

print("Silver consumer started...")
logging.info("Silver consumer started")

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def normalize_text(value):
    """
    lower + trim text
    """
    if pd.isna(value):
        return None
    return str(value).strip().lower()


def parse_datetime(value):
    """
    Convert datetime to standard format
    YYYY-MM-DD HH:MM:SS
    """
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None


def safe_read_csv(path):
    """
    Read csv safely
    """
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except:
            return pd.DataFrame()
    return pd.DataFrame()


# ==========================================================
# TRANSFORM FUNCTIONS
# ==========================================================

def transform_911(record):
    """
    Clean 911 record
    """

    df = pd.DataFrame([record])

    # required fields
    if "incident_number" not in df.columns:
        return pd.DataFrame()

    # standardize columns if missing
    if "neighborhood" not in df.columns:
        df["neighborhood"] = None

    if "datetime" not in df.columns:
        df["datetime"] = None

    # clean
    df["neighborhood"] = df["neighborhood"].apply(normalize_text)
    df["datetime"] = df["datetime"].apply(parse_datetime)

    # drop null
    df = df.dropna(subset=["incident_number", "neighborhood", "datetime"])

    # dedup inside batch
    df = df.drop_duplicates(subset=["incident_number"])

    return df


def transform_crime(record):
    """
    Clean crime record
    """

    df = pd.DataFrame([record])

    if "report_number" not in df.columns:
        return pd.DataFrame()

    if "neighborhood" not in df.columns:
        df["neighborhood"] = None

    if "report_date_time" not in df.columns:
        df["report_date_time"] = None

    if "offense_sub_category" not in df.columns:
        df["offense_sub_category"] = "UNKNOWN"

    # clean
    df["neighborhood"] = df["neighborhood"].apply(normalize_text)
    df["report_date_time"] = df["report_date_time"].apply(parse_datetime)

    df["offense_sub_category"] = df["offense_sub_category"].fillna("UNKNOWN")

    # drop null
    df = df.dropna(subset=["report_number", "neighborhood", "report_date_time"])

    # dedup
    df = df.drop_duplicates(subset=["report_number"])

    return df


# ==========================================================
# SAVE FUNCTIONS
# ==========================================================

def append_merge_csv(new_df, file_path, pk):
    """
    Merge old csv + new records + dedup
    """

    if new_df.empty:
        return

    old_df = safe_read_csv(file_path)

    merged = pd.concat([old_df, new_df], ignore_index=True)

    merged = merged.drop_duplicates(subset=[pk], keep="last")

    merged.to_csv(file_path, index=False)

    logging.info(f"Updated {file_path} rows={len(merged)}")
    print(f"Updated {file_path} rows={len(merged)}")


# ==========================================================
# KAFKA CONSUMER
# ==========================================================

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="silver-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# ==========================================================
# MAIN LOOP
# ==========================================================

buffer_911 = []
buffer_crime = []

BATCH_SIZE = 100
LAST_FLUSH = time.time()
FLUSH_INTERVAL = 10   # seconds


def flush_all():
    global buffer_911, buffer_crime

    # ---------- 911 ----------
    if buffer_911:
        df = pd.concat(buffer_911, ignore_index=True)
        df = df.drop_duplicates(subset=["incident_number"])
        append_merge_csv(df, SILVER_911, "incident_number")
        buffer_911 = []

    # ---------- Crime ----------
    if buffer_crime:
        df = pd.concat(buffer_crime, ignore_index=True)
        df = df.drop_duplicates(subset=["report_number"])
        append_merge_csv(df, SILVER_CRIME, "report_number")
        buffer_crime = []


try:
    for message in consumer:

        topic = message.topic
        data = message.value

        try:

            # ==========================================
            # 911 TOPIC
            # ==========================================
            if topic == "bronze_911_calls":
                clean_df = transform_911(data)

                if not clean_df.empty:
                    buffer_911.append(clean_df)

            # ==========================================
            # CRIME TOPIC
            # ==========================================
            elif topic == "bronze_crime_reports":
                clean_df = transform_crime(data)

                if not clean_df.empty:
                    buffer_crime.append(clean_df)

            # ==========================================
            # FLUSH CONDITION
            # ==========================================
            now = time.time()

            total_rows = len(buffer_911) + len(buffer_crime)

            if total_rows >= BATCH_SIZE or (now - LAST_FLUSH) >= FLUSH_INTERVAL:
                flush_all()
                LAST_FLUSH = now

        except Exception as e:
            logging.error(f"Transform error: {str(e)}")

except KeyboardInterrupt:
    print("Stopping consumer...")
    flush_all()

except Exception as e:
    logging.error(f"Fatal error: {str(e)}")