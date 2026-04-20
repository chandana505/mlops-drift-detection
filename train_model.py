import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

# Load dataset
df = pd.read_csv("creditcard.csv")

# Drop label column if exists
if "Class" in df.columns:
    X = df.drop(columns=["Class"])
else:
    X = df

# Train model
model = IsolationForest(contamination=0.02, random_state=42)
model.fit(X)

# Save model
joblib.dump(model, "model.pkl")

print("Model trained and saved!")