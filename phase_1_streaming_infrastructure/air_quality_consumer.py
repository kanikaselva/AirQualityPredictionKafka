from kafka import KafkaConsumer
import json
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Custom deserializer that safely handles empty messages
def safe_json_deserializer(v):
    if not v:
        return None
    decoded = v.decode('utf-8').strip()
    if not decoded:
        return None
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        # Optionally log invalid message
        logging.warning(f"Invalid JSON skipped: {decoded}")
        return None

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'air_quality',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='air_quality_group',
    value_deserializer=safe_json_deserializer
)

print("Partitions:", consumer.partitions_for_topic('air_quality'))

# Consume messages
for message in consumer:
    if message.value is None:
        continue
    print(f"Received: {message.value}")
    logging.info(f"Processed message: {message.value}")

consumer.close()