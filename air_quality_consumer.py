from kafka import KafkaConsumer
import json
import logging

# Setup logging for monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'air_quality',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',      # Start from beginning of topic if offsets unknown
    enable_auto_commit=True,
    group_id='air_quality_group',
    value_deserializer=lambda v: json.loads(v).decode('utf-8')
)

print(consumer.partitions_for_topic('air_quality'))

def consume_messages():
    print("Starting consumer...")
    try:
        for message in consumer:
            print(f"Received: {message.value}")
            logging.info(f"Processed message: {message.value}")
    except KeyboardInterrupt:
        print("Consumer stopped by user.")

if __name__ == "__main__":
    consume_messages()