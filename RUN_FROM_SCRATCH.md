# Run From Scratch (Exact Commands)

This guide provides the exact commands to set up and run the app from a fresh start.

## 0) Prerequisites

- Docker Desktop installed and running
- Python 3.10+ installed
- `creditcard.csv` downloaded and placed in the project root
  - Kaggle: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

---

## 1) Open project folder

```bash
cd /c/playground/mlops-drift-detection
```

---

## 2) Create and activate Python virtual environment

```bash
python -m venv .venv
source .venv/Scripts/activate
```

---

## 3) Install Python dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 4) Start Kafka with Docker (KRaft mode)

```bash
docker-compose up -d
```

Verify Kafka container:

```bash
docker ps
```

Verify topic exists:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

You should see `transactions` in the output.

---

## 5) Train the model

```bash
python train_model.py
```

This creates `model.pkl`.

---

## 6) Run the pipeline (recommended single command)

```bash
python main.py
```

What this does:
- Starts producer and consumer together
- Producer streams data into Kafka and injects synthetic drift
- Consumer reads batches and logs anomaly + drift metrics

Stop with `Ctrl+C`.

---

## 7) Optional: Run producer and consumer separately

Terminal 1:

```bash
source .venv/Scripts/activate
python consumer.py
```

Terminal 2:

```bash
source .venv/Scripts/activate
python producer.py
```

---

## 8) Optional: Configure email alerts

Create a `.env` file in project root:

```env
ALERT_EMAIL_SENDER=your_email@gmail.com
ALERT_EMAIL_RECEIVER=receiver_email@gmail.com
ALERT_EMAIL_PASSWORD=your_gmail_app_password
```

Then run as usual:

```bash
python main.py
```

---

## 9) Stop and clean Docker services

```bash
docker-compose down
```

If you also want to remove volumes:

```bash
docker-compose down -v
```

---

## 10) Quick rerun commands (after first setup)

```bash
cd /c/playground/mlops-drift-detection
source .venv/Scripts/activate
docker-compose up -d
python main.py
```
