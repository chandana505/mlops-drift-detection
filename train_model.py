import os
import logging
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

CSV_PATH = "creditcard.csv"

if not os.path.exists(CSV_PATH):
    logger.error("Dataset not found: %s. Please download it from Kaggle and place it in the project root.", CSV_PATH)
    raise FileNotFoundError(f"{CSV_PATH} not found. Download from https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud")

# Load dataset
df = pd.read_csv(CSV_PATH)

# Train IsolationForest ONLY on normal transactions (Class==0)
# so it learns what "normal" looks like and can flag anomalies/fraud better.
if "Class" in df.columns:
    X_normal = df[df["Class"] == 0].drop(columns=["Class"])
else:
    X_normal = df

logger.info("Training on %d normal records...", len(X_normal))

model = IsolationForest(contamination=0.02, random_state=42)
model.fit(X_normal)

# Save model
joblib.dump(model, "model.pkl")

logger.info("Model trained and saved to model.pkl")
