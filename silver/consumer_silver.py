import os
import json
import time
import logging
import re
import pandas as pd
from kafka import KafkaConsumer

# ==================================================
# CONFIG
# ==================================================

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

# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    filename=f"{LOG_DIR}/consumer_silver.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

print("Silver consumer started...")
logging.info("Silver consumer started")

# ==================================================
# COMMON CLEANERS
# ==================================================

BAD_VALUES = {"", "-", "--", "nan", "none", "null", "999", "unknown"}


def clean_text(val, lower=True):
    if pd.isna(val):
        return None

    val = str(val).strip()

    if val.lower() in BAD_VALUES:
        return None

    if re.fullmatch(r"\d+", val):
        return None

    return val.lower() if lower else val.upper()


def clean_category(val):
    if pd.isna(val):
        return "UNKNOWN"

    val = str(val).strip()

    if val.lower() in BAD_VALUES:
        return "UNKNOWN"

    if re.fullmatch(r"\d+", val):
        return "UNKNOWN"

    return val.upper()


def parse_datetime(val):
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def safe_read_csv(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except:
            return pd.DataFrame()
    return pd.DataFrame()

# ==================================================
# TRANSFORM 911
# ==================================================

def transform_911(record):
    df = pd.DataFrame([record])

    required = ["incident_number", "neighborhood", "datetime"]

    for col in required:
        if col not in df.columns:
            df[col] = None

    df["incident_number"] = df["incident_number"].astype(str).str.strip()
    df["neighborhood"] = df["neighborhood"].apply(clean_text)
    df["datetime"] = df["datetime"].apply(parse_datetime)

    df = df.dropna(subset=["incident_number", "neighborhood", "datetime"])
    df = df.drop_duplicates(subset=["incident_number"])

    return df

# ==================================================
# TRANSFORM CRIME
# ==================================================

def transform_crime(record):
    df = pd.DataFrame([record])

    required = [
        "report_number",
        "neighborhood",
        "report_date_time",
        "offense_sub_category"
    ]

    for col in required:
        if col not in df.columns:
            df[col] = None

    df["report_number"] = df["report_number"].astype(str).str.strip()
    df["neighborhood"] = df["neighborhood"].apply(clean_text)
    df["report_date_time"] = df["report_date_time"].apply(parse_datetime)
    df["offense_sub_category"] = df["offense_sub_category"].apply(clean_category)

    df = df.dropna(subset=[
        "report_number",
        "neighborhood",
        "report_date_time"
    ])

    df = df.drop_duplicates(subset=["report_number"])

    return df

# ==================================================
# SAVE
# ==================================================

def save_merge(df_new, path, pk):
    if df_new.empty:
        return

    df_old = safe_read_csv(path)

    merged = pd.concat([df_old, df_new], ignore_index=True)
    merged = merged.drop_duplicates(subset=[pk], keep="last")

    merged.to_csv(path, index=False)

    print(f"Updated {path} rows={len(merged)}")
    logging.info(f"Updated {path} rows={len(merged)}")

# ==================================================
# FLUSH
# ==================================================

buffer_911 = []
buffer_crime = []

def flush():
    global buffer_911, buffer_crime

    if buffer_911:
        df = pd.concat(buffer_911, ignore_index=True)
        df = df.drop_duplicates(subset=["incident_number"])
        save_merge(df, SILVER_911, "incident_number")
        buffer_911 = []

    if buffer_crime:
        df = pd.concat(buffer_crime, ignore_index=True)
        df = df.drop_duplicates(subset=["report_number"])
        save_merge(df, SILVER_CRIME, "report_number")
        buffer_crime = []

# ==================================================
# KAFKA
# ==================================================

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="silver-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# ==================================================
# MAIN LOOP
# ==================================================

BATCH_SIZE = 100
FLUSH_INTERVAL = 10
last_flush = time.time()

try:
    for msg in consumer:

        topic = msg.topic
        data = msg.value

        try:
            if topic == "bronze_911_calls":
                df = transform_911(data)
                if not df.empty:
                    buffer_911.append(df)

            elif topic == "bronze_crime_reports":
                df = transform_crime(data)
                if not df.empty:
                    buffer_crime.append(df)

            total = len(buffer_911) + len(buffer_crime)
            now = time.time()

            if total >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL:
                flush()
                last_flush = now

        except Exception as e:
            logging.error(f"Transform Error: {e}")

except KeyboardInterrupt:
    flush()

except Exception as e:
    logging.error(f"Fatal Error: {e}")