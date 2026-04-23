# Adaptive MLOps for Drift Detection using Kafka

## 📌 Introduction

Machine Learning (ML) models deployed in real-world environments often face performance degradation over time due to changes in incoming data. These changes, commonly referred to as **data drift** and **concept drift**, can significantly impact the reliability of predictions.

This project presents an **Adaptive MLOps framework** designed to monitor streaming data, detect drift, and trigger alerts in real time. The system integrates **Apache Kafka** for data streaming, statistical techniques for drift detection, and machine learning for anomaly detection.

---

## 🎯 Problem Statement

In production environments:

* Data distributions evolve over time
* Models trained on historical data become less accurate
* There is often no real-time monitoring system
* Failures are detected too late

👉 Therefore, there is a need for a system that can:

* Continuously monitor incoming data
* Detect deviations from expected behavior
* Alert stakeholders proactively

---

## 🎯 Objectives

* Implement real-time data streaming using Kafka
* Detect **data quality issues** (missing values, outliers)
* Identify **numerical drift** using statistical methods
* Detect **categorical drift** in data distributions
* Monitor **model performance** using anomaly detection
* Generate **automated alerts** for critical events

---

## 🧠 Key Concepts

### 1. MLOps

MLOps (Machine Learning Operations) is a discipline that combines machine learning, DevOps, and data engineering to automate and monitor ML systems in production.

It focuses on:

* Continuous integration and deployment (CI/CD)
* Monitoring model performance
* Managing data pipelines

---

### 2. Data Drift

Data drift occurs when the statistical properties of input data change over time.

#### Types:

* **Covariate Drift**: Change in input features
* **Prior Probability Drift**: Change in class distribution
* **Concept Drift**: Relationship between input and output changes

---

### 3. Population Stability Index (PSI)

PSI measures the change in distribution between two datasets.

* PSI < 0.1 → No drift
* 0.1 ≤ PSI ≤ 0.25 → Moderate drift
* PSI > 0.25 → Significant drift

Used for **numerical feature monitoring**

---

### 4. Chi-Square Test

A statistical test used to compare categorical distributions.

* Measures deviation between expected and observed frequencies
* Low p-value (< 0.05) indicates significant drift

---

### 5. Anomaly Detection (Isolation Forest)

Isolation Forest is an unsupervised learning algorithm that:

* Detects unusual patterns in data
* Works without labeled data
* Is efficient for large datasets

---

## 🏗️ System Architecture

Producer → Kafka → Consumer → ML Model → Drift Detection → Alert System

### Components:

* **Producer**: Streams transaction data into Kafka
* **Kafka Broker**: Handles real-time data ingestion
* **Consumer**: Processes incoming data in batches
* **ML Model**: Detects anomalies
* **Drift Detection Module**: Applies statistical techniques
* **Alert System**: Sends notifications

---

## 🔄 Data Pipeline

1. Historical dataset is used to train the model
2. Streaming data is generated and sent to Kafka
3. Consumer reads data in batches
4. Data quality checks are performed
5. Drift detection algorithms are applied
6. Model predictions are generated
7. Alerts are triggered if thresholds are exceeded

---

## 📊 Features

### ✅ Data Quality Monitoring

* Missing value detection
* Outlier detection

### ✅ Drift Detection

* PSI for numerical features
* Chi-square for categorical features

### ✅ Model Monitoring

* Anomaly detection using Isolation Forest
* Tracks anomaly rate

### ✅ Real-Time Alerting

* Email notifications using SMTP
* Immediate response to system issues

---

## 🛠️ Technologies Used

* **Apache Kafka** – Real-time streaming platform
* **Python** – Core programming language
* **Pandas & NumPy** – Data processing
* **Scikit-learn** – Machine learning
* **SciPy** – Statistical testing
* **SMTP** – Email alert system

---

## 📥 Dataset

The system uses the **Credit Card Fraud Detection dataset**.

Source:
https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

⚠️ Note: Dataset is not included due to size constraints.

---

## ⚙️ Implementation Steps

1. Train the model using historical data (`model.py`)
2. Start Kafka and create topic
3. Stream data using producer (`producer.py`)
4. Process data using consumer (`consumer.py`)
5. Apply monitoring and drift detection
6. Trigger alerts when thresholds are exceeded

---

## 📧 Alert Mechanism

The system uses SMTP to send email alerts when:

* Data drift is detected
* High anomaly rate is observed
* Data quality issues occur

This ensures **real-time monitoring and quick response**

---

## 📈 Results and Observations

* The system successfully detects drift in streaming data
* PSI and Chi-square effectively identify distribution changes
* Isolation Forest identifies anomalous patterns
* Email alerts provide timely notifications

---

## ⚠️ Challenges Faced

* Kafka setup on Windows environment
* Handling large dataset constraints
* Email authentication configuration

---

## 🔮 Future Scope

* Automated model retraining
* Integration with dashboards (Grafana, Power BI)
* Deployment on cloud platforms (AWS, Azure)
* Real-time fraud detection systems

---

## ✅ Conclusion

This project demonstrates a complete **real-time MLOps monitoring system** that ensures the reliability and robustness of machine learning models.

By combining streaming technologies, statistical methods, and machine learning, the system provides a scalable and effective solution for monitoring production ML systems.

---

## 👤 Author

Mani Chandana

---
