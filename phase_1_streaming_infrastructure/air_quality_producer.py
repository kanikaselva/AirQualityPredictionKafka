from ucimlrepo import fetch_ucirepo 
import pandas as pd
import time
from kafka import KafkaProducer
import json
  
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

# Stream each row
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('air_quality', value=data)
    print(f"Sent: {data}")
    time.sleep(1)  # simulate real-time streaming

producer.flush()
producer.close()