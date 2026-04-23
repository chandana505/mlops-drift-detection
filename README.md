# MLOps Drift Detection (Kafka + Isolation Forest)

## Project Overview

This repository implements a **local, streaming MLOps demo** for fraud-anomaly monitoring and drift detection on the credit card dataset.

For exact first-time setup commands, see `RUN_FROM_SCRATCH.md`.

Pipeline flow:
1. `train_model.py` trains an `IsolationForest` on normal (`Class == 0`) transactions.
2. `producer.py` streams records to Kafka topic `transactions` and injects synthetic drift after a configured index.
3. `consumer.py` consumes records in batches, runs inference, computes quality + drift metrics, and logs results.
4. `main.py` runs producer and consumer concurrently with graceful shutdown.

---

## Current Implementation (As in Code)

### Model
- Algorithm: `IsolationForest(contamination=0.02, random_state=42)`
- Training data: only normal transactions (`Class == 0`)
- Artifact output: `model.pkl`

### Streaming
- Kafka topic: `transactions`
- Producer payload: row data + `drift_injected` flag
- Synthetic drift: for records `i >= PRODUCER_DRIFT_START`, `Amount *= PRODUCER_DRIFT_SCALE`

### Batch Monitoring in Consumer
For each batch (default `100` rows), the consumer logs:
- Missing value count
- Outlier count (`> mean + 3 * std` over numeric columns)
- Fraud metrics:
  - Batch Precision / Recall / F1 (shown as `N/A` if no fraud positives in batch)
  - Cumulative Precision / Recall / F1 over bounded history (`deque(maxlen=10000)`)
- Anomaly rate (fraction predicted as anomaly)
- PSI on `Amount` using quantile bins (`bins=5`)
- Chi-square p-value on `Amount` categories:
  - `Low < 100`, `100 <= Medium < 1000`, `High >= 1000`
- Drift classification state and drift precision/recall vs synthetic ground truth
- False alarms (drift FP count)
- Batch latency and latency overhead vs baseline
- Latest producer sent-count from shared queue

### Drift Decision Logic
- `PSI_THRESHOLD = 0.30`
- `CHI_P_THRESHOLD = 0.05 / 3` (Bonferroni correction)
- Drift confirmation requires **2 consecutive** raw drift batches
- Alert cooldown: **5 batches**

### Email Alerts (Optional)
`send_email_alert()` is active, but sends email only when `ALERT_EMAIL_PASSWORD` is set.

Alert triggers in code:
- Missing values > 0
- Outliers > 50
- Anomaly rate > 0.10
- Confirmed drift with `actual_drift=True`

---

## Repository Structure

```
mlops-drift-detection/
├── main.py
├── train_model.py
├── producer.py
├── consumer.py
├── logging_config.py
├── docker-compose.yaml
├── requirements.txt
├── PROJECT_PROPOSAL.md
├── README.md
├── creditcard.csv
├── logs/
└── model.pkl (generated)
```

---

## Prerequisites

- Python 3.10+
- Docker + Docker Compose
- `creditcard.csv` in project root

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Run Kafka (KRaft, No Zookeeper)

```bash
docker-compose up -d
```

Verify topic:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Stop services:

```bash
docker-compose down
```

---

## Run the Pipeline

### 1) Train model

```bash
python train_model.py
```

### 2) Run producer + consumer together

```bash
python main.py
```

### Alternative: run separately

Consumer:

```bash
python consumer.py
```

Producer:

```bash
python producer.py
```

---

## Environment Variables

Loaded via `python-dotenv` (`.env` supported).

### Producer
- `PRODUCER_MAX_RECORDS` (default `2000`)
- `PRODUCER_DRIFT_START` (default `1000`)
- `PRODUCER_DRIFT_SCALE` (default `5.0`)
- `PRODUCER_SLEEP` (default `0.005`)

### Consumer
- `CONSUMER_GROUP_ID` (default `drift-detection-group`)
- `CONSUMER_AUTO_OFFSET_RESET` (default `earliest`)
- `CONSUMER_BATCH_SIZE` (default `100`)
- `REFERENCE_REFRESH_EVERY` (default `0`, fixed PSI baseline)
- `LATENCY_BASELINE_BATCHES` (default `5`)

### Email
- `ALERT_EMAIL_SENDER` (default `mtechproject2001@gmail.com`)
- `ALERT_EMAIL_RECEIVER` (default `mtechproject2001@gmail.com`)
- `ALERT_EMAIL_PASSWORD` (required for SMTP send)

---

## Logging

- Configured in `logging_config.py`
- Console + rotating file logs (`logs/run_YYYYMMDD_HHMMSS.log`)
- Rotation: 10 MB/file, 5 backups
- UTF-8 file encoding

---

## Scope and Limitations

Implemented in this repo:
- Streaming inference + drift monitoring + synthetic drift evaluation

Not implemented in current code:
- Automated model retraining/orchestration
- Model registry / canary deployment
- Multi-layer drift ensemble beyond PSI + chi-square

See `PROJECT_PROPOSAL.md` for a realistic roadmap from the current baseline.