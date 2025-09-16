from ucimlrepo import fetch_ucirepo 
import pandas as pd
import time
from kafka import KafkaProducer
import json
import logging
  
# fetch dataset 
air_quality = fetch_ucirepo(id=360) 
  
# data (as pandas dataframes) 
X = air_quality.data.features 
y = air_quality.data.targets 

# Convert to DataFrame
df = pd.DataFrame(air_quality.data.features, columns=air_quality.data.feature_names)
  
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate streaming with batching
batch_size = 100   # send 100 rows per simulated "batch"
interval = 1      # delay between sends (1 second)


buffer = []

for index, row in df.iterrows():
    data = row.to_dict()
    buffer.append(data)

    # Once buffer is full, flush batch
    if len(buffer) >= batch_size:
        for record in buffer:
            producer.send("air_quality", value=record)
        logging.info(f"Sent batch of {len(buffer)} records")
        buffer.clear()
        time.sleep(interval)  # simulate real-time delay

# Send leftover records if any
if buffer:
    for record in buffer:
        producer.send("air_quality", value=record)
    logging.info(f"Sent final batch of {len(buffer)} records")

producer.flush()
producer.close()