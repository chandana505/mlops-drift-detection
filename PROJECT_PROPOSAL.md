# Project Proposal: Streaming Drift Detection Baseline and Evolution Plan

## 1. Title

**Real-Time Fraud-Anomaly Monitoring with Synthetic Drift Injection on Kafka Streams**

---

## 2. Problem Statement

Fraud detection models degrade when transaction distributions shift over time. In streaming environments, this degradation can happen before periodic retraining cycles catch it. A practical first step is to build a low-latency monitoring loop that:

1. runs continuously on streaming batches,
2. detects early distribution shift signals,
3. reports detection quality against known drift events.

This repository currently implements that baseline loop end-to-end.

---

## 3. What Is Implemented Today (Current Code)

### 3.1 Architecture

- **Kafka broker (KRaft mode)** via `docker-compose.yaml`
- **Producer (`producer.py`)** publishes records from `creditcard.csv` to topic `transactions`
- **Consumer (`consumer.py`)** ingests batches and computes inference + drift metrics
- **Model training (`train_model.py`)** builds `IsolationForest` and saves `model.pkl`
- **Single entrypoint (`main.py`)** runs producer + consumer concurrently with graceful shutdown

### 3.2 Synthetic Drift Injection

Producer marks each message with `drift_injected`:
- Before drift start: `drift_injected=False`
- After drift start: `Amount *= PRODUCER_DRIFT_SCALE`, `drift_injected=True`

This gives ground truth for objective drift detection evaluation.

### 3.3 Detection and Monitoring Methods

Consumer computes, per batch:
- Data quality: missing values, outliers
- Model outputs: anomaly rate from Isolation Forest
- Drift signals:
  - **PSI** on `Amount` (quantile bins)
  - **Chi-square** on `Amount` categories (`Low`, `Medium`, `High`)
- Drift state machine:
  - PSI threshold: `0.30`
  - Chi-square threshold: `0.05/3`
  - Confirmed drift requires 2 consecutive drift-positive batches
- Evaluation metrics against `drift_injected` ground truth:
  - TP / FP / TN / FN
  - Drift precision / recall
  - False alarms
- Operational metrics:
  - batch latency
  - latency overhead vs baseline (non-drift early batches)

### 3.4 Alerting

Email alerts are implemented (Gmail SMTP) and triggered for:
- missing values,
- high outliers,
- high anomaly rate,
- confirmed drift.

Alerts are sent only when `ALERT_EMAIL_PASSWORD` is configured.

---

## 4. Objectives and Evaluation Targets (Baseline Phase)

For the current implementation, this proposal tracks practical baseline goals:

1. **Reliable drift signal generation** on synthetic drift windows.
2. **Controlled false alarms** via confirmation and cooldown logic.
3. **Low processing overhead** suitable for local near-real-time execution.
4. **Reproducible experiment runs** through environment-based configuration.

Suggested baseline acceptance checks:
- Drift recall > 0.90 on injected-drift sections
- False alarm rate lower than raw (unconfirmed) thresholding
- Stable per-batch processing time under demo load

---

## 5. Configuration Surface (Implemented)

### Producer Parameters
- `PRODUCER_MAX_RECORDS` (default `2000`)
- `PRODUCER_DRIFT_START` (default `1000`)
- `PRODUCER_DRIFT_SCALE` (default `5.0`)
- `PRODUCER_SLEEP` (default `0.005`)

### Consumer Parameters
- `CONSUMER_GROUP_ID` (default `drift-detection-group`)
- `CONSUMER_AUTO_OFFSET_RESET` (default `earliest`)
- `CONSUMER_BATCH_SIZE` (default `100`)
- `REFERENCE_REFRESH_EVERY` (default `0`, fixed PSI baseline)
- `LATENCY_BASELINE_BATCHES` (default `5`)

### Alert Parameters
- `ALERT_EMAIL_SENDER`
- `ALERT_EMAIL_RECEIVER`
- `ALERT_EMAIL_PASSWORD`

---

## 6. Known Limitations of Current Baseline

1. Drift detection is based on **univariate `Amount` drift** and simple categorization.
2. No automated retraining loop exists yet.
3. No model registry, canary deployment, or rollback orchestration.
4. Single-node local Kafka is intended for development/demo, not production scale.
5. Classification quality is constrained by extreme class imbalance and unsupervised modeling.

---

## 7. Proposed Next Phases

### Phase 1 (Completed): Streaming Baseline
- Kafka producer/consumer
- Isolation Forest inference
- PSI + chi-square drift detection
- Synthetic drift evaluation and logging

### Phase 2: Drift Robustness Improvements
- Multifeature PSI / distance metrics
- Better drift calibration from rolling historical quantiles
- Dedicated drift report summary per run

### Phase 3: Adaptive Response
- Retraining trigger policy from sustained drift + quality degradation
- Automated retraining job (offline) and model replacement workflow

### Phase 4: Productionization
- Containerized app services
- Persistent metrics backend + dashboarding
- CI validation on drift scenarios

---

## 8. Expected Outcome

This project delivers a practical MLOps baseline that can **detect and quantify distribution drift in streaming fraud-like data** with transparent metrics and reproducible behavior. It is intentionally scoped as a strong monitoring foundation before introducing full adaptive retraining automation.

---

## 9. Repository Mapping

- `train_model.py` — train and export `IsolationForest`
- `producer.py` — stream data + inject drift + publish progress
- `consumer.py` — batch scoring, drift logic, evaluation metrics
- `main.py` — run producer and consumer threads together
- `logging_config.py` — centralized rotating logs
- `docker-compose.yaml` — local Kafka (KRaft)
- `README.md` — usage and configuration guide

---

## 10. Status

- **Document Version:** 2.0
- **Last Updated:** 2026-04-24
- **Status:** Aligned with current repository implementation