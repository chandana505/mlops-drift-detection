import logging
import json
import os
import queue
from collections import deque
import pandas as pd
import numpy as np
import joblib
from scipy.stats import chisquare
import smtplib
from email.mime.text import MIMEText
import time
from dotenv import load_dotenv
from sklearn.metrics import precision_score, recall_score, f1_score
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

# Load environment variables when consumer is run directly
load_dotenv()

# ================= EMAIL =================
def send_email_alert(message):
    sender = os.getenv("ALERT_EMAIL_SENDER", "mtechproject2001@gmail.com")
    receiver = os.getenv("ALERT_EMAIL_RECEIVER", "mtechproject2001@gmail.com")
    password = os.getenv("ALERT_EMAIL_PASSWORD")

    if not password:
        logger.warning("[ALERT] Email password not set (ALERT_EMAIL_PASSWORD). Skipping email: %s", message)
        return

    msg = MIMEText(message)
    msg['Subject'] = "MLOps Alert"
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.send_message(msg)
        logger.info("[ALERT] Email sent: %s", message)
    except Exception as e:
        logger.error("Email failed: %s", e, exc_info=True)


# ================= PSI =================
def calculate_psi(expected, actual, bins=5):
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

# ================= CONSUMER =================
def _get_latest_queue_value(q):
    """Drain queue and return the last item, or None if empty."""
    latest = None
    while True:
        try:
            latest = q.get_nowait()
        except queue.Empty:
            break
    return latest


def _safe_div(numerator, denominator):
    return numerator / denominator if denominator else 0.0


def _format_metric(value, decimals=3):
    return "N/A" if value is None else f"{value:.{decimals}f}"


def run_consumer(stop_event=None, metrics_queue=None):
    consumer_group_id = os.getenv("CONSUMER_GROUP_ID", "drift-detection-group")
    offset_reset = os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest")
    batch_size = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    reference_refresh_every = int(os.getenv("REFERENCE_REFRESH_EVERY", "0"))

    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset=offset_reset,
        enable_auto_commit=True,
        group_id=consumer_group_id
    )

    # Batch size is configurable for demo vs. analysis runs.
    BATCH_SIZE = batch_size
    batch = []
    reference_batch = None

    # Global metric accumulators for classification (bounded to prevent memory leaks)
    MAX_HISTORY = 10_000
    all_y_true = deque(maxlen=MAX_HISTORY)
    all_y_pred = deque(maxlen=MAX_HISTORY)

    # Drift evaluation tracking (bounded to prevent memory leaks)
    drift_tp_total = 0
    drift_fp_total = 0
    drift_fn_total = 0
    drift_tn_total = 0
    batch_index = 0

    # Latency baseline for overhead reporting
    BASELINE_LATENCY_BATCHES = int(os.getenv("LATENCY_BASELINE_BATCHES", "5"))
    baseline_latencies = deque(maxlen=max(1, BASELINE_LATENCY_BATCHES))

    # Drift confirmation & alert tuning parameters
    DRIFT_CONFIRMATION_BATCHES = 2      # Require N consecutive batches above threshold
    ALERT_COOLDOWN_BATCHES = 5          # Prevent repeated alerts for N batches after alert
    PSI_THRESHOLD = 0.30                # Slightly higher than 0.25 to reduce FPR
    CHI_P_THRESHOLD = 0.05 / 3          # Bonferroni correction for 3 categories (Low/Medium/High)

    consecutive_drift_batches = 0
    drift_active = False
    alert_cooldown = 0
    last_idle_log_time = time.time()
    last_data_time = time.time()
    IDLE_LOG_INTERVAL_SECONDS = 5
    PARTIAL_BATCH_LOG_STEP = 25


    logger.info(
        "[OK] Consumer started... waiting for data (group_id=%s, auto_offset_reset=%s, batch_size=%d)",
        consumer_group_id,
        offset_reset,
        BATCH_SIZE,
    )

    try:
        while stop_event is None or not stop_event.is_set():
            raw_msgs = consumer.poll(timeout_ms=1000)
            if raw_msgs:
                last_data_time = time.time()

            if not raw_msgs:
                now = time.time()
                if now - last_idle_log_time >= IDLE_LOG_INTERVAL_SECONDS:
                    logger.info("[WAIT] No new Kafka messages yet. Current partial batch=%d/%d", len(batch), BATCH_SIZE)
                    last_idle_log_time = now

                if now - last_data_time >= 120:
                    logger.warning("[TIMEOUT] No data received for 2 minutes. Shutting down consumer.")
                    if stop_event is not None:
                        stop_event.set()
                    break


            for topic_partition, messages in raw_msgs.items():
                for msg in messages:
                    batch.append(msg.value)
                    if len(batch) % PARTIAL_BATCH_LOG_STEP == 0 and len(batch) < BATCH_SIZE:
                        logger.debug("[RECV] Collected %d/%d records for current batch", len(batch), BATCH_SIZE)

                    if len(batch) >= BATCH_SIZE:
                        batch_start = time.time()

                        df_batch = pd.DataFrame(batch)

                        # ===== DATA QUALITY =====
                        missing = df_batch.isnull().sum().sum()

                        numeric_cols = df_batch.select_dtypes(include=[np.number]).columns
                        outliers = (
                            df_batch[numeric_cols] >
                            df_batch[numeric_cols].mean() + 3 * df_batch[numeric_cols].std()
                        ).sum().sum()

                        if missing > 0:
                            send_email_alert("Missing values detected!")

                        if outliers > 50:
                            send_email_alert("Too many outliers detected!")

                        # ===== CLASSIFICATION METRICS (accumulated globally) =====
                        y_true = df_batch["Class"].values if "Class" in df_batch.columns else None
                        df_model = df_batch.drop(columns=["Class", "drift_injected"], errors="ignore")
                        preds = model.predict(df_model)

                        # IsolationForest: -1 = anomaly (fraud), 1 = normal
                        # Dataset Class: 1 = fraud, 0 = normal
                        y_pred = np.where(preds == -1, 1, 0)

                        batch_fraud_count = int((y_true == 1).sum()) if y_true is not None else 0
                        batch_prec = batch_rec = batch_f1 = None
                        cum_prec = cum_rec = cum_f1 = None
                        if y_true is not None:
                            if batch_fraud_count > 0:
                                batch_prec = precision_score(y_true, y_pred, zero_division=0)
                                batch_rec = recall_score(y_true, y_pred, zero_division=0)
                                batch_f1 = f1_score(y_true, y_pred, zero_division=0)

                            all_y_true.extend(y_true.tolist())
                            all_y_pred.extend(y_pred.tolist())

                            if int(np.sum(all_y_true)) > 0:
                                cum_prec = precision_score(all_y_true, all_y_pred, zero_division=0)
                                cum_rec = recall_score(all_y_true, all_y_pred, zero_division=0)
                                cum_f1 = f1_score(all_y_true, all_y_pred, zero_division=0)

                        # ===== ANOMALY RATE =====
                        anomaly_rate = (preds == -1).sum() / len(preds)

                        if anomaly_rate > 0.1:
                            send_email_alert("High anomaly rate detected!")

                        # ===== PSI =====
                        if reference_batch is None:
                            reference_batch = df_batch[
                                [c for c in df_batch.columns if c not in ["drift_injected"]]
                            ].copy()
                            psi = 0.0
                        else:
                            psi = calculate_psi(reference_batch["Amount"], df_batch["Amount"])

                        # Keep a fixed baseline by default for stable demo logs.
                        if reference_refresh_every > 0 and batch_index > 0 and batch_index % reference_refresh_every == 0:
                            reference_batch = df_batch[
                                [c for c in df_batch.columns if c not in ["drift_injected"]]
                            ].copy()

                        # ===== CHI-SQUARE =====
                        df_batch["Amount_Category"] = df_batch["Amount"].apply(categorize_amount)
                        ref_for_chi = reference_batch.copy()
                        ref_for_chi["Amount_Category"] = ref_for_chi["Amount"].apply(categorize_amount)

                        exp = ref_for_chi["Amount_Category"].value_counts().sort_index()
                        act = df_batch["Amount_Category"].value_counts().sort_index()

                        exp, act = exp.align(act, fill_value=0)

                        # Avoid divide-by-zero when a category has 0 expected count
                        exp_safe = exp.replace(0, 1e-6)
                        chi_stat, p_val = chisquare(f_obs=act, f_exp=exp_safe)

                        # ===== DRIFT CONFIRMATION & ALERT STATE MACHINE =====
                        raw_predicted = (psi > PSI_THRESHOLD) or (p_val < CHI_P_THRESHOLD)

                        if raw_predicted:
                            consecutive_drift_batches += 1
                        else:
                            consecutive_drift_batches = 0

                        confirmed_drift = consecutive_drift_batches >= DRIFT_CONFIRMATION_BATCHES

                        # ===== DRIFT DETECTION METRICS (batch-level) =====
                        actual_drift = any(m.get("drift_injected", False) for m in batch)

                        batch_drift_tp = int(confirmed_drift and actual_drift)
                        batch_drift_fp = int(confirmed_drift and not actual_drift)
                        batch_drift_fn = int((not confirmed_drift) and actual_drift)
                        batch_drift_tn = int((not confirmed_drift) and (not actual_drift))

                        drift_tp_total += batch_drift_tp
                        drift_fp_total += batch_drift_fp
                        drift_fn_total += batch_drift_fn
                        drift_tn_total += batch_drift_tn

                        # Only alert on real drift-confirmed batches.
                        if confirmed_drift and actual_drift and not drift_active:
                            drift_active = True
                            if alert_cooldown == 0:
                                send_email_alert("Drift confirmed!")
                                alert_cooldown = ALERT_COOLDOWN_BATCHES
                        elif not confirmed_drift and drift_active:
                            drift_active = False
                            logger.info("Drift state cleared.")

                        if alert_cooldown > 0:
                            alert_cooldown -= 1

                        drift_precision = _safe_div(drift_tp_total, drift_tp_total + drift_fp_total)
                        drift_recall = _safe_div(drift_tp_total, drift_tp_total + drift_fn_total)
                        false_alarm_count = drift_fp_total

                        # ===== LATENCY OVERHEAD =====
                        batch_end_time = time.time()
                        latency = batch_end_time - batch_start
                        if not actual_drift and len(baseline_latencies) < BASELINE_LATENCY_BATCHES:
                            baseline_latencies.append(latency)
                        baseline_latency = float(np.mean(baseline_latencies)) if baseline_latencies else None
                        latency_overhead = (latency - baseline_latency) if baseline_latency is not None else None
                        latency_overhead_pct = (
                            _safe_div(latency_overhead, baseline_latency) * 100.0
                            if baseline_latency is not None else None
                        )


                        # ===== PRODUCER SENT COUNT =====
                        sent_count = _get_latest_queue_value(metrics_queue)
                        sent_str = f"[Producer: {sent_count} records sent]" if sent_count is not None else ""

                        # ===== CONSOLIDATED LOG LINE =====
                        logger.info(
                            "Batch %04d | Missing=%d Outliers=%d | FraudN=%d | "
                            "AnomRate=%.3f PSI=%.4f ChiP=%.4f | "
                            "BatchP=%s BatchR=%s BatchF1=%s CumP=%s CumR=%s CumF1=%s | "
                            "Drift=%s DriftP=%s DriftR=%s FalseAlarms=%d | "
                            "Latency=%.4f LatOver=%s LatOverPct=%s %s",
                            batch_index,
                            missing,
                            outliers,
                            batch_fraud_count,
                            anomaly_rate,
                            psi,
                            p_val,
                            _format_metric(batch_prec),
                            _format_metric(batch_rec),
                            _format_metric(batch_f1),
                            _format_metric(cum_prec),
                            _format_metric(cum_rec),
                            _format_metric(cum_f1),
                            "YES" if confirmed_drift else "NO",
                            _format_metric(drift_precision),
                            _format_metric(drift_recall),
                            false_alarm_count,
                            latency,
                            _format_metric(latency_overhead, 4),
                            _format_metric(latency_overhead_pct, 2),
                            sent_str
                        )

                        batch = []
                        batch_index += 1
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)
    run_consumer()
