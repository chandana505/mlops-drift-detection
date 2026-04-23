from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
import joblib
from scipy.stats import chisquare
import smtplib
from email.mime.text import MIMEText

# ================= EMAIL =================
def send_email_alert(message):
    sender = "mtechproject2001@gmail.com"
    receiver = "mtechproject2001@gmail.com"
    password = "isgmiekokrszgrkp"

    msg = MIMEText(message)
    msg['Subject'] = "🚨 MLOps Alert"
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.send_message(msg)
        print("📧 Email sent:", message)
    except Exception as e:
        print("Email failed:", e)

# ================= PSI =================
def calculate_psi(expected, actual, bins=10):
    expected = np.array(expected)
    actual = np.array(actual)

    breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    expected_perc = expected_counts / len(expected)
    actual_perc = actual_counts / len(actual)

    psi = np.sum((actual_perc - expected_perc) *
                 np.log((actual_perc + 1e-6) / (expected_perc + 1e-6)))

    return psi

# ================= CATEGORY =================
def categorize_amount(amount):
    if amount < 100:
        return "Low"
    elif amount < 1000:
        return "Medium"
    else:
        return "High"

# ================= LOAD MODEL =================
model = joblib.load("model.pkl")

# ================= KAFKA =================
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

batch = []
BATCH_SIZE = 100
reference_batch = None

print("✅ Consumer started... waiting for data")

for msg in consumer:
    batch.append(msg.value)

    if len(batch) >= BATCH_SIZE:
        print("\n==========================")
        print("Processing batch...")

        df_batch = pd.DataFrame(batch)

        # ===== DATA QUALITY =====
        missing = df_batch.isnull().sum().sum()
        print("Missing values:", missing)

        numeric_cols = df_batch.select_dtypes(include=[np.number]).columns
        outliers = (df_batch[numeric_cols] > 
                    df_batch[numeric_cols].mean() + 3 * df_batch[numeric_cols].std()).sum().sum()
        print("Outliers:", outliers)

        if missing > 0:
            send_email_alert("Missing values detected!")

        if outliers > 50:
            send_email_alert("Too many outliers detected!")

        # ===== MODEL =====
        df_model = df_batch.drop(columns=["Class"], errors="ignore")
        preds = model.predict(df_model)

        anomaly_rate = (preds == -1).sum() / len(preds)
        print("Anomaly Rate:", anomaly_rate)

        if anomaly_rate > 0.1:
            send_email_alert("High anomaly rate detected!")

        # ===== PSI =====
        if reference_batch is None:
            reference_batch = df_batch.copy()
            print("Reference batch set")
        else:
            psi = calculate_psi(reference_batch["Amount"], df_batch["Amount"])
            print("PSI:", psi)

            if psi > 0.25:
                send_email_alert("Numerical drift detected!")

        # ===== CHI-SQUARE =====
        df_batch["Amount_Category"] = df_batch["Amount"].apply(categorize_amount)
        reference_batch["Amount_Category"] = reference_batch["Amount"].apply(categorize_amount)

        exp = reference_batch["Amount_Category"].value_counts().sort_index()
        act = df_batch["Amount_Category"].value_counts().sort_index()

        exp, act = exp.align(act, fill_value=0)

        chi_stat, p_val = chisquare(f_obs=act, f_exp=exp)
        print("Chi-square p-value:", p_val)

        if p_val < 0.05:
            send_email_alert("Categorical drift detected!")

        batch = []