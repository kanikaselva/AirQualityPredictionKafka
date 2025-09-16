from kafka import KafkaConsumer
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Functions

def safe_json_deserializer(v):
    """ Safely deserialize JSON, returning None on failure """
    if not v:
        return None
    decoded = v.decode('utf-8').strip()
    if not decoded:
        return None
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        logging.warning(f"Invalid JSON skipped: {decoded}")
        return None

def validate_message(msg: dict) -> bool:
    """ Basic schema validation """
    expected_cols = 15
    if not isinstance(msg, dict):
        return False
    if len(msg.keys()) != expected_cols:
        logging.warning(f"Unexpected schema length: {len(msg)} instead of {expected_cols}")
        return False
    return True

def preprocess_message(msg: dict) -> dict:
    """ This function replaces -200 values with NaN"""
    return {k: (np.nan if v == -200 else v) for k, v in msg.items()}

def save_partitioned(df, base_dir="../data"):
    """
    Save DataFrame to CSV, partitioned by month-year from 'Date' column.
    Assumes 'Date' column is in DD/MM/YYYY format.
    """
    # Convert Date to datetime
    df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y", errors='coerce')

    # Extract month-year for partitioning
    df['year_month'] = df['Date'].dt.strftime("%Y-%m")

    # Save per month
    for ym, group in df.groupby('year_month'):
        os.makedirs(base_dir, exist_ok=True)
        file_path = os.path.join(base_dir, f"air_quality_{ym}.csv")

        # If file exists, append without header
        if os.path.exists(file_path):
            group.to_csv(file_path, index=False, mode='a', header=False)
        else:
            group.to_csv(file_path, index=False, mode='w', header=True)

        logging.info(f"Saved {len(group)} rows to {file_path}")


# Consuming messages
consumer = KafkaConsumer(
    'air_quality',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,   # disable auto-commit ()
    group_id='air_quality_group',
    value_deserializer=safe_json_deserializer
)

BATCH_SIZE = 100
buffer = []

try:
    for message in consumer:
        if message.value is None:
            continue

        if not validate_message(message.value):
            continue

        clean_msg = preprocess_message(message.value)
        buffer.append(clean_msg)

        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)
            save_partitioned(df)
            buffer = []

            consumer.commit()  # commit offset after successful save

except KeyboardInterrupt:
    logging.info("Consumer stopped by user.")
finally:
    # Flush remaining buffer on exit
    if buffer:
        df = pd.DataFrame(buffer)
        save_partitioned(df)
    consumer.close()
    logging.info("Consumer closed.")