import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

# Load dataset
df = pd.read_csv("creditcard.csv").head(5000)

# Prepare data
if "Class" in df.columns:
    X = df.drop(columns=["Class"])
else:
    X = df

# Train model
model = IsolationForest(contamination=0.02, random_state=42)
model.fit(X)

# Save model
joblib.dump(model, "model.pkl")

print("Model trained and saved as model.pkl")