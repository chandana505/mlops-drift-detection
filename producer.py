from kafka import KafkaProducer
import pandas as pd
import json
import time

# Load dataset
df = pd.read_csv("creditcard.csv").head(5000)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "transactions"

for _, row in df.iterrows():
    data = row.to_dict()

    # 🔥 Optional: force drift for demo
    # data["Amount"] = data["Amount"] * 5

    producer.send(topic, value=data)
    print("Sent record")
    time.sleep(0.01)

producer.flush()