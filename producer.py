from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load dataset
df = pd.read_csv("creditcard.csv")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "transactions"

for _, row in df.iterrows():
    data = row.to_dict()
    producer.send(topic, value=data)
    print("Sent")

    time.sleep(0.01)  # simulate streaming

producer.flush()