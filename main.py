import pandas as pd
import numpy as np
import time
from sklearn.ensemble import IsolationForest
from scipy.stats import chisquare
import smtplib
from email.mime.text import MIMEText

# ========================
# EMAIL ALERT FUNCTION
# ========================
def send_email_alert(message):
    sender = "mtechproject2001@gmail.com"
    receiver = "mtechproject2001@gmail.com"
    password = "isgmiekokrszgrkp"

    msg = MIMEText(message)
    msg['Subject'] = "🚨 MLOps Alert"
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
        print("📧 Email sent:", message)
    except Exception as e:
        print("Email failed:", e)

# ========================
# LOAD DATA
# ========================
df = pd.read_csv("creditcard.csv").head(5000)

if "Class" in df.columns:
    X = df.drop(columns=["Class"])
else:
    X = df

# ========================
# TRAIN MODEL
# ========================
model = IsolationForest(contamination=0.02, random_state=42)
model.fit(X)

print("Model trained!")

# ========================
# PSI FUNCTION
# ========================
def calculate_psi(expected, actual, bins=10):
    expected = np.array(expected)
    actual = np.array(actual)

    breakpoints = np.linspace(0, 100, bins + 1)
    breakpoints = np.percentile(expected, breakpoints)

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    expected_perc = expected_counts / len(expected)
    actual_perc = actual_counts / len(actual)

    psi = np.sum((actual_perc - expected_perc) *
                 np.log((actual_perc + 1e-6) / (expected_perc + 1e-6)))

    return psi

# ========================
# CATEGORIZATION
# ========================
def categorize_amount(amount):
    if amount < 100:
        return "Low"
    elif amount < 1000:
        return "Medium"
    else:
        return "High"

# ========================
# STREAMING SIMULATION
# ========================
BATCH_SIZE = 100
batch = []
reference_batch = None

for _, row in df.iterrows():
    batch.append(row.to_dict())

    if len(batch) >= BATCH_SIZE:
        print("\n==============================")
        print("Processing new batch...")

        df_batch = pd.DataFrame(batch)

        # ========================
        # DATA QUALITY
        # ========================
        missing_values = df_batch.isnull().sum().sum()
        print("Missing values:", missing_values)

        if missing_values > 0:
            send_email_alert("Missing values detected!")

        outliers = (df_batch > df_batch.mean() + 3 * df_batch.std()).sum().sum()
        print("Outliers:", outliers)

        if outliers > 50:
            send_email_alert("Too many outliers detected!")

        # ========================
        # ML MODEL
        # ========================
        df_model = df_batch.copy()

        if "Class" in df_model.columns:
            df_model = df_model.drop(columns=["Class"])

        preds = model.predict(df_model)
        anomaly_rate = (preds == -1).sum() / len(preds)

        print("Anomaly Rate:", anomaly_rate)

        if anomaly_rate > 0.1:
            send_email_alert("High anomaly rate detected! Possible concept drift.")

        # ========================
        # PSI DRIFT
        # ========================
        if reference_batch is None:
            reference_batch = df_batch.copy()
            print("Reference batch set")
        else:
            psi_value = calculate_psi(reference_batch["Amount"], df_batch["Amount"])
            print("PSI:", psi_value)

            if psi_value > 0.25:
                send_email_alert("Significant numerical drift detected (PSI)!")

        # ========================
        # CHI-SQUARE DRIFT
        # ========================
        df_batch["Amount_Category"] = df_batch["Amount"].apply(categorize_amount)
        reference_batch["Amount_Category"] = reference_batch["Amount"].apply(categorize_amount)

        expected_counts = reference_batch["Amount_Category"].value_counts().sort_index()
        actual_counts = df_batch["Amount_Category"].value_counts().sort_index()

        expected_counts, actual_counts = expected_counts.align(actual_counts, fill_value=0)

        chi_stat, p_value = chisquare(f_obs=actual_counts, f_exp=expected_counts)

        print("Chi-square p-value:", p_value)

        if p_value < 0.05:
            send_email_alert("Categorical drift detected (Chi-square)!")

        batch = []
        time.sleep(1)