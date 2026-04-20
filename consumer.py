from kafka import KafkaConsumer
import json
import pandas as pd
import joblib

# Load model
model = joblib.load("model.pkl")

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

batch = []
BATCH_SIZE = 100

for message in consumer:
    batch.append(message.value)

    if len(batch) >= BATCH_SIZE:
        print("\nProcessing new batch...")

        df_batch = pd.DataFrame(batch)

        # Drop label if exists
        if "Class" in df_batch.columns:
            df_batch = df_batch.drop(columns=["Class"])

        # ML Prediction
        preds = model.predict(df_batch)

        anomaly_rate = (preds == -1).sum() / len(preds)

        print("Anomaly Rate:", anomaly_rate)

        batch = []