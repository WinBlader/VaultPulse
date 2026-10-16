# 🏧 VaultPulse AI
## Smart ATM Cash Demand Forecasting & Dynamic Replenishment Platform

**Big Data Analytics (BDA) Capstone Project · Project Statement 47**

VaultPulse AI is a Big Data Analytics project that simulates ATM withdrawal activity, streams transactions through Apache Kafka, stores transaction data in date-partitioned storage, forecasts upcoming cash demand with a Random Forest model, and recommends replenishment levels through a web dashboard.

> **Dataset note:** The project uses synthetic/simulated ATM transaction data. The transaction patterns are designed to resemble common banking demand patterns; this should not be described as a live bank dataset or as directly imported Kaggle data unless that data has actually been integrated.

---

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Problem Statement](#2-problem-statement)
- [3. Objectives](#3-objectives)
- [4. Key Features](#4-key-features)
- [5. Technology Stack](#5-technology-stack)
- [6. System Architecture](#6-system-architecture)
- [7. End-to-End Implementation](#7-end-to-end-implementation)
- [8. Dataset](#8-dataset)
- [9. Forecasting and Replenishment Logic](#9-forecasting-and-replenishment-logic)
- [10. Project Structure](#10-project-structure)
- [11. Requirements](#11-requirements)
- [12. Run with Docker (Recommended)](#12-run-with-docker-recommended)
- [13. HDFS Commands](#13-hdfs-commands)
- [14. Kafka Commands](#14-kafka-commands)
- [15. Application and API Access](#15-application-and-api-access)
- [16. Useful Docker Commands](#16-useful-docker-commands)
- [17. Run Locally Without Docker](#17-run-locally-without-docker)
- [18. Model Evaluation](#18-model-evaluation)
- [19. Limitations](#19-limitations)
- [20. Troubleshooting](#20-troubleshooting)
- [21. Future Enhancements](#21-future-enhancements)
- [22. Team and Credits](#22-team-and-credits)

---

## 1. Project Overview

ATM operators need to keep enough cash in each machine to meet customer demand while avoiding unnecessary cash sitting idle. VaultPulse AI demonstrates a data-driven approach to this problem.

The system simulates withdrawal transactions, sends events through Kafka, writes transaction batches to a date-partitioned data lake, and uses historical demand patterns to forecast the next day's cash requirement. A replenishment layer applies a safety buffer and generates recommendations that can be viewed in the dashboard.

### Main workflow

1. Simulate ATM withdrawal transactions.
2. Publish withdrawal events to an Apache Kafka topic.
3. Consume events and store batches in date-partitioned storage.
4. Use historical data to build forecasting features.
5. Train or load a Random Forest regression model.
6. Forecast demand for each ATM.
7. Calculate target cash and recommended replenishment.
8. Display forecasts, ATM risk status, and live events in the dashboard.

## 2. Problem Statement

Traditional ATM replenishment may rely on fixed schedules or static cash quantities. A single fixed quantity does not reflect the different needs of a busy transit station, a shopping mall, a business district, and a residential area.

This can cause two main issues:

- **Cash-out / stockout:** An ATM runs out of cash before the next refill.
- **Excess idle cash:** Too much money remains unused in a low-demand ATM, tying up cash and increasing cash-handling overhead.

VaultPulse AI explores how streaming data, historical transaction storage, forecasting, and replenishment rules can help make ATM cash planning more responsive.

## 3. Objectives

- Simulate ATM transactions with timestamps, amounts, ATM identifiers, and transaction details.
- Demonstrate event streaming using Apache Kafka.
- Demonstrate date-partitioned transaction storage using HDFS in the Docker setup.
- Build time-based features from historical demand.
- Forecast next-day cash demand using a Random Forest Regressor.
- Add a configurable safety-stock buffer to the forecast.
- Classify ATM cash positions and generate replenishment recommendations.
- Provide a web dashboard for visualizing forecasts, transactions, and recommendations.
- Run the components together using Docker Compose.

## 4. Key Features

- **Synthetic transaction generator:** Creates sample ATM withdrawal events.
- **Kafka streaming pipeline:** Uses the `atm-withdrawal-transactions` topic for transaction events.
- **Date-partitioned storage:** Organizes transaction files by year, month, and day.
- **Forecasting model:** Uses a Scikit-learn Random Forest Regressor.
- **Feature engineering:** Includes lagged demand, rolling statistics, and calendar features.
- **Replenishment rules:** Adds a configurable 15% safety buffer to predicted demand.
- **Risk labels:** Supports labels such as `STOCKOUT_ALERT`, `EXCESS_CASH_ALERT`, and `OPTIMAL`.
- **Web dashboard:** Displays key metrics, demand charts, recommendations, and recent events.
- **WebSocket updates:** Supports a live transaction feed when the application pipeline is running.
- **Docker Compose setup:** Runs the application and supporting services together.

---

## 5. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Programming language | Python 3.10+ | Data generation, backend, and ML pipeline |
| Streaming | Apache Kafka | Publishing and consuming withdrawal events |
| Kafka coordination | Apache ZooKeeper | Coordination for the ZooKeeper-based Kafka configuration |
| Big Data storage | Apache Hadoop HDFS | Storing transaction files in the Docker deployment |
| Machine learning | Scikit-learn | Random Forest demand forecasting |
| Backend | FastAPI | Dashboard and API backend |
| ASGI server | Uvicorn | Serving the FastAPI application |
| Frontend | HTML, CSS, JavaScript | Web operations dashboard |
| Charts | Chart.js | Demand and forecast visualizations |
| Live updates | WebSockets | Streaming transaction events to connected clients |
| Containers | Docker, Docker Compose | Building and running the multi-service setup |

---

## 6. System Architecture

```text
             ┌────────────────────────────┐
             │ Synthetic ATM Transactions │
             │ ATM ID, time, amount, etc. │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │       Kafka Producer       │
             │ atm-withdrawal-transactions│
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │       Kafka Consumer       │
             │ Batch and route events     │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │      Hadoop HDFS           │
             │ Date-partitioned CSV files │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │ Feature Engineering        │
             │ Lags, rolling and calendar │
             │ features                   │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │ Random Forest Regressor    │
             │ Next-day demand forecast   │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │ Replenishment Decision     │
             │ Forecast + safety buffer  │
             │ Cash-position risk labels  │
             └──────────────┬─────────────┘
                            │
                            ▼
             ┌────────────────────────────┐
             │ FastAPI + WebSocket        │
             │ Dashboard, charts, actions │
             └────────────────────────────┘
```

### Docker services

The Docker Compose configuration for this project is expected to run these five services:

| Service/container | Purpose | Common exposed port |
|---|---|---:|
| `vaultpulse-zookeeper` | Kafka coordination | `2181` |
| `vaultpulse-kafka` | Kafka broker | `9092`, `29092` |
| `vaultpulse-namenode` | HDFS namespace and web UI | `9000`, `9870` |
| `vaultpulse-datanode` | HDFS data storage | `9864` |
| `vaultpulse-app` | FastAPI application, dashboard, and project pipeline | `8000` |

Port mappings can differ if `docker-compose.yml` has been changed.

---

## 7. End-to-End Implementation

### 7.1 Transaction generation

The application generates synthetic ATM withdrawal events. Depending on the generator configuration, records can include an ATM identifier, timestamp, withdrawal amount, card type, and transaction status. The generated data is intended for demonstration and experimentation, not real banking operations.

### 7.2 Kafka ingestion

The producer publishes events to the Kafka topic:

```text
atm-withdrawal-transactions
```

The consumer reads the events, collects records into batches, and routes them to storage and any configured live dashboard feed.

### 7.3 HDFS storage

Transaction data is organized into date partitions. A typical partition layout is:

```text
/hdfs/atm_transactions/
└── year=2026/
    └── month=10/
        ├── day=15/
        │   └── part-00000.csv
        └── day=16/
            └── part-00000.csv
```

The actual root path depends on the configured HDFS setup. In the current Docker environment, the observed path is `/hdfs/atm_transactions/...`. Use the commands in the HDFS section to verify the live path.

### 7.4 Feature engineering

Historical demand is transformed into features that help the model learn recent demand and recurring patterns. Features described by the project include:

- `lag_1`: demand from the previous day.
- `lag_2`: demand from two days earlier.
- `lag_7`: demand from the same weekday in the previous week.
- `rolling_mean_7`: seven-day moving average.
- `rolling_std_7`: seven-day demand variability.
- `day_of_week_num`: numeric weekday.
- `is_weekend`: weekend indicator.
- `is_holiday`: holiday or configured high-demand-day indicator.
- `day_of_month`: calendar day.
- ATM identifier features: distinguish individual ATM demand baselines.

The README describes 14 engineered features overall; the exact final feature list should match the feature-building code in `forecasting/`.

### 7.5 Forecasting

A Random Forest Regressor learns the relationship between historical demand features and cash demand. The model produces a demand estimate for the next planning period, such as the following day.

### 7.6 Replenishment recommendation

The forecast is passed to a decision layer. The layer applies a safety buffer, compares target cash with the ATM's current cash, and generates a recommended replenishment amount and risk label.

### 7.7 Dashboard

The FastAPI-powered web application exposes data to the frontend. The dashboard is designed to show operational metrics, forecast charts, replenishment recommendations, and recent transactions. A WebSocket endpoint is documented as `/ws/transactions` for live transaction updates.

---

## 8. Dataset

### Dataset source

The project uses **synthetic ATM transaction data**. The data generator is designed to simulate patterns such as weekday/weekend differences, payday spikes, and variation between ATM locations. It is not a live bank transaction dataset.

The project's existing documentation describes a scale of approximately 104,364 simulated withdrawals and 450+ daily summary records across 90+ operating days. Treat these as project-reported figures and verify them against the generated data before using them as final experimental results.

### Example ATM profiles

| ATM profile | Typical pattern |
|---|---|
| Transit station | High demand around commuter periods |
| Shopping mall | Potentially higher weekend demand |
| Business district | Stronger weekday demand |
| Residential area | Lower, steadier demand |
| Airport | Demand distributed across a longer operating day |

These profiles are illustrative synthetic scenarios, not measured behavior from real ATMs.

### Common fields

The dataset documentation lists fields such as:

| Field | Description |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `atm_id` | ATM identifier |
| `atm_name` | ATM display name |
| `location` | Location or environment profile |
| `timestamp` | Transaction date and time |
| `amount_withdrawn` | Amount dispensed |
| `card_type` | Card network/type |
| `day_of_week` | Weekday name |
| `is_weekend` | Weekend indicator |
| `is_holiday` | Holiday or configured special-day indicator |
| `remaining_cash` | Simulated cash balance after a transaction |
| `transaction_status` | Success/failure status |

The actual columns should be confirmed against the CSV header in the generated dataset.

---

## 9. Forecasting and Replenishment Logic

### 9.1 Target cash level

Let:

- \(D_{i,t+1}\) be the predicted demand for ATM \(i\) in the next period.
- \(s\) be the safety-stock rate, set to `0.15` in the documented configuration.

Then:

```text
Target Cash = Forecasted Demand × (1 + Safety Stock Rate)
```

For a 15% buffer:

```text
Target Cash = Forecasted Demand × 1.15
```

**Example:** If the forecast is 20,000 currency units, the target cash level is 23,000.

### 9.2 Recommended replenishment

Let:

- `Target Cash` be the calculated target.
- `Current Cash` be the simulated cash currently in the ATM.
- `Max Capacity` be the ATM's configured maximum capacity, if enforced.

A general replenishment rule is:

```text
Required Cash = max(0, Target Cash - Current Cash)
Recommended Cash = min(Required Cash, Max Capacity - Current Cash)
```

The capacity limit should only be applied if it is implemented and configured in the code. The exact formula in the running implementation is authoritative.

### 9.3 Risk labels

The documented labels are:

- `STOCKOUT_ALERT`: current cash may be insufficient for predicted demand.
- `EXCESS_CASH_ALERT`: current cash is substantially above the target.
- `OPTIMAL`: the current cash position is within the configured acceptable range.

Thresholds are rule-based and should be verified in the implementation before presenting them as validated operational policy.

---

## 10. Project Structure

The repository may evolve; use `ls` or `find` to verify exact filenames in your current checkout.

```text
VaultPulse/
├── dashboard/
│   ├── app.py
│   └── templates/
│       └── index.html
├── forecasting/
│   └── saved_models/
│       └── model_metrics.json
├── hdfs_storage/
│   └── hdfs_data/
│       └── atm_transactions/
├── kafka_pipeline/
├── docker-compose.yml
├── Dockerfile
├── docker_start.bat
├── run_project.py
└── README.md
```

Directory names and individual files may differ between repository versions.

---

## 11. Requirements

### Recommended: Docker

- Docker Desktop installed and running.
- Docker Compose available through `docker compose`.
- Internet access for the initial image builds and dependency downloads.
- Sufficient local disk space for images, containers, and generated transaction data.

Once the images and dependencies have been downloaded, a previously built stack may be able to start without internet access, provided no new downloads are required.

### Local Python run

For running outside Docker, the README documents Python 3.10+ and the project's Python dependencies. Check the repository's `requirements.txt` or project setup files and install the dependencies listed there. Kafka and HDFS functionality may require their services to be available, unless the application's fallback mode is enabled.

---

## 12. Run with Docker (Recommended)

Run commands from the repository root—the folder containing `docker-compose.yml`.

### 12.1 Check Docker

```bash
docker --version
docker compose version
docker info
```

### 12.2 Build and start the services

```bash
docker compose up --build -d
```

If the repository includes the Windows batch file, it may also be started with:

```bash
./docker_start.bat
```

### 12.3 Check container status

```bash
docker compose ps
```

Or:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### 12.4 View application logs

```bash
docker compose logs -f app
```

If the Compose service name is not `app`, use the service name shown by `docker compose ps` or inspect the Compose file. You can also use the known container name:

```bash
docker logs -f vaultpulse-app
```

### 12.5 Open the application

- Dashboard/API: http://localhost:8000
- HDFS NameNode web UI: http://localhost:9870

The web interfaces are available only while the corresponding containers are running and their ports are mapped.

### 12.6 Stop the services

```bash
docker compose down
```

To stop the containers and also remove named/anonymous volumes managed by Compose:

```bash
docker compose down --volumes
```

**Caution:** Removing volumes may delete persistent HDFS data. Use the second command only when you intentionally want to remove stored data.

---

## 13. HDFS Commands

These commands assume the NameNode container is named `vaultpulse-namenode`.

### 13.1 HDFS cluster report

```bash
docker exec -it vaultpulse-namenode hdfs dfsadmin -report
```

### 13.2 List the HDFS root

```bash
docker exec -it vaultpulse-namenode hdfs dfs -ls /
```

### 13.3 List all files under `/hdfs`

```bash
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /hdfs
```

### 13.4 List ATM transaction partitions

```bash
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /hdfs/atm_transactions
```

### 13.5 Check storage usage

```bash
docker exec -it vaultpulse-namenode hdfs dfs -du -h /hdfs/atm_transactions
```

### 13.6 Read a partition file

Example for 16 October 2026:

```bash
docker exec -it vaultpulse-namenode hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=16/part-00000.csv
```

To display only the first few lines in Git Bash:

```bash
docker exec vaultpulse-namenode hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=16/part-00000.csv | head
```

To count data rows excluding the header in a Bash-compatible terminal:

```bash
docker exec vaultpulse-namenode bash -lc "hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=16/part-00000.csv | tail -n +2 | wc -l"
```

Replace the date and filename with a partition that exists in your HDFS listing.

### 13.7 Open the HDFS web interface

Visit:

```text
http://localhost:9870
```

Then open **Utilities → Browse the file system** to inspect the stored directories and files.

---

## 14. Kafka Commands

The topic documented for the project is:

```text
atm-withdrawal-transactions
```

### 14.1 List topics

```bash
docker exec -it vaultpulse-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 14.2 Describe the transaction topic

```bash
docker exec -it vaultpulse-kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic atm-withdrawal-transactions
```

### 14.3 Read events from the beginning

```bash
docker exec -it vaultpulse-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic atm-withdrawal-transactions --from-beginning
```

Stop the consumer with `Ctrl+C`.

> Kafka command paths and listener addresses can vary by image and configuration. If a command is not found, inspect the Kafka container image and its available scripts. In some setups, the scripts may be under `/opt/kafka/bin/`.

---

## 15. Application and API Access

### Dashboard

```text
http://localhost:8000
```

### Common documented endpoints

| Endpoint | Intended purpose |
|---|---|
| `/api/status` | Application/status information |
| `/api/recommendations` | Replenishment recommendations |
| `/api/chart-data` | Data for dashboard charts |
| `/ws/transactions` | WebSocket transaction feed |
| `/api/hdfs/ingest-partition` | On-demand partition ingestion, if enabled in the current build |

The exact routes may differ by code version. Check the FastAPI application route definitions for the authoritative endpoint list.

If FastAPI's interactive documentation is enabled, try:

```text
http://localhost:8000/docs
```

---

## 16. Useful Docker Commands

### View all logs

```bash
docker compose logs
```

### Follow logs for all services

```bash
docker compose logs -f
```

### Restart the stack

```bash
docker compose restart
```

### Restart only the app container

```bash
docker restart vaultpulse-app
```

### Inspect CPU and memory use

```bash
docker stats --no-stream
```

### List Docker images

```bash
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.ID}}"
```

### Open a shell inside the NameNode container

```bash
docker exec -it vaultpulse-namenode bash
```

When finished, exit the container shell:

```bash
exit
```

### Rebuild after code or configuration changes

```bash
docker compose up --build -d
```

---

## 17. Run Locally Without Docker

The repository documentation includes a local runner:

```bash
python run_project.py
```

Run this command from the project root and only if `run_project.py` exists in your checkout.

If the project uses a virtual environment, create and activate one first.

**Windows PowerShell:**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

**Git Bash:**

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
```

Install the dependencies from the repository's dependency file, if present:

```bash
pip install -r requirements.txt
```

Then run:

```bash
python run_project.py
```

If the local runner is not available or requires separate services, follow the repository's current setup scripts and configuration. Docker Compose is the recommended path for demonstrating the full Kafka/HDFS setup.

---

## 18. Model Evaluation

The project's existing documentation reports the following example metrics:

| Metric | Reported value |
|---|---:|
| R² score | Approximately 0.836 |
| Mean Absolute Error (MAE) | Approximately 5,183 currency units |
| Mean Absolute Percentage Error (MAPE) | Approximately 16.4% |

These values are **reported project metrics, not independently verified results in this README**. Re-run the model evaluation and check `forecasting/saved_models/model_metrics.json` before using them in a final report or viva. Performance on synthetic data does not establish performance on real-world banking data.

### What the metrics mean

- **R²:** Measures how well the model explains variation in the target on the evaluation data.
- **MAE:** Average absolute difference between predicted and actual demand, in the target's units.
- **MAPE:** Average percentage error; interpretation can be problematic when actual values are zero or close to zero.

For a fair evaluation, use a time-ordered train/test split and avoid using future information when constructing lag or rolling features.

---

## 19. Limitations

- The transaction dataset is synthetic and may not represent real customer behavior.
- Forecast quality depends on the amount and realism of generated historical data.
- Reported metrics must be checked against the latest model run.
- Replenishment thresholds and safety-stock rates are configurable rules, not a substitute for bank-approved operational policies.
- The Docker topology is a demonstration environment and should not be treated as a production banking deployment.
- Real deployment would require authentication, authorization, secrets management, audit logging, monitoring, secure networking, and validation with real operational data.

---

## 20. Troubleshooting

### Docker command fails

Confirm Docker Desktop is running, then check:

```bash
docker info
docker compose ps
```

### App is not reachable on port 8000

Check container status and logs:

```bash
docker ps
docker logs --tail 200 vaultpulse-app
```

Check whether port `8000` is already used by another process and confirm the port mapping in `docker-compose.yml`.

### HDFS path is missing

List the HDFS root and available project directories:

```bash
docker exec -it vaultpulse-namenode hdfs dfs -ls /
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /hdfs
```

Do not assume the root is `/atm_transactions`; in the current documented environment, transaction partitions were observed under `/hdfs/atm_transactions`.

### Kafka topic is missing

List the available topics:

```bash
docker exec -it vaultpulse-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Check Kafka and application logs:

```bash
docker logs --tail 200 vaultpulse-kafka
docker logs --tail 200 vaultpulse-app
```

### README merge conflict in Git

After editing a conflicted file and confirming the contents are correct:

```bash
git status
git add README.md
git commit -m "Resolve README merge conflict"
git push -u origin main
```

Do not run the commit until all merge conflicts are resolved.

### Windows line-ending warnings

Warnings that LF may be replaced by CRLF are common in Windows Git setups. They are not necessarily errors and do not, by themselves, indicate that the application is broken.

---

## 21. Future Enhancements

- Evaluate the model on verified, anonymized real-world data where legally permitted.
- Compare Random Forest with baseline forecasting methods and other time-series models.
- Add model monitoring and scheduled retraining.
- Add configurable ATM capacity and replenishment constraints.
- Include route planning and vehicle scheduling for cash delivery.
- Add authentication, role-based access, and audit trails.
- Add automated tests and CI checks.
- Add data-quality checks and schema validation to the streaming pipeline.
- Improve observability with structured logs and service health checks.

---

## 22. Team and Credits

VaultPulse AI is a Big Data Analytics capstone project. Update this section with the final team member names, roles, institution, and acknowledgements required by your submission guidelines.

---

## License

Add the license chosen for this repository before redistributing or reusing the project.
