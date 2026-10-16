🏧 VaultPulse AI: Smart ATM Cash Demand Forecasting & Dynamic Replenishment Platform

Big Data Analytics (BDA) Capstone Project • Project Statement 47

Streaming Ingestion (Apache Kafka) • Distributed Data Lake (Hadoop HDFS) • Machine Learning (Time-Series Forecasting) • Docker Multi-Container Architecture • Real-Time Replenishment Dashboard

📌 1. Project Overview & Problem Statement

❌ The Industry Problem: Fixed-Schedule Refills

Commercial bank cash operations teams traditionally replenish ATMs on a blind, fixed schedule (e.g., loading a flat $35,000 into every ATM every Monday morning). This rigid schedule completely ignores localized demand patterns and causes two severe operational failures:

1. Cash-Outs / Stockouts at High-Traffic Machines: ATMs located at transit hubs, shopping malls, and airports experience surges in customer cash withdrawals. Under a flat schedule, these machines run completely dry within hours, causing customer dissatisfaction, lost interchange fee revenue, and severe bank SLA penalty fines.

2. Idle Excess Cash at Low-Traffic Machines: ATMs in quiet residential neighborhoods receive the same $35,000 refill despite dispensing only $8,000 to $10,000 per week. Tens of thousands of dollars sit idle inside vault cassettes earning 0% interest, incurring high vault insurance premiums, and locking up liquid bank reserves that could otherwise earn yield.

💡 The Solution: VaultPulse AI

VaultPulse AI replaces dumb fixed schedules with an automated, closed-loop Big Data Analytics & Machine Learning pipeline:

Real-Time Streaming: Captures customer cash withdrawals in real time via Apache Kafka (atm-withdrawal-transactions).

Big Data Storage: Persists and partitions transaction streams inside Hadoop HDFS (/atm_transactions/year=YYYY/month=MM/day=DD/).

Time-Series ML Forecasting: Automatically trains a Random Forest Regressor over historical HDFS partitions using 14 lag and calendar features to predict tomorrow's exact cash demand per ATM.

Smart Replenishment Optimizer: Adds a dynamic 15% safety stock buffer, classifies machine risk (STOCKOUT_ALERT, EXCESS_CASH_ALERT, OPTIMAL), and recommends the exact replenishment cash to load into armored delivery vans.

Docker Multi-Container Cluster: Fully orchestrated in Docker Compose running 5 independent services (Zookeeper, Kafka, Hadoop NameNode, Hadoop DataNode, VaultPulse App).

Interactive Operations Dashboard: An asynchronous, glassmorphism web console with live Chart.js demand curves, real-time WebSocket transaction tickers, one-click Cash Van dispatch, dynamic HDFS partition ingestion, and model retraining.

📖 2. Terminology Demystified (Simple & Intuitive)

| Term | Simple Definition |

| :--- | :--- |

| Stockout / Cash-Out | The ATM vault is completely empty ($0). Customers cannot withdraw money and leave frustrated. |

| Idle Excess Cash | Surplus money sitting trapped inside a quiet ATM doing nothing. The bank loses interest and incurs vault insurance costs. |

| Safety Stock Buffer | A 15% protective cash cushion calculated on top of the forecasted demand to safely absorb unexpected withdrawal surges. |

| Target Cash Level | The optimal cash volume needed in an ATM for tomorrow: Forecasted Demand + 15% Safety Stock. |

| Recommended Replenishment | The exact dollar amount the bank's armored cash delivery van should physically load into the machine tomorrow. |

| Apache Kafka | A distributed, high-speed streaming message highway that captures and buffers withdrawal events instantaneously as they occur. |

| Hadoop HDFS | A fault-tolerant Big Data filesystem that partitions millions of transactions into organized date folders (year=YYYY/month=MM/day=DD/). |

| Docker Compose | A tool for defining and running multi-container Docker applications with isolated networking and storage. |

| Time-Series Lag Features | Feeding the ML model historical withdrawal numbers from yesterday (Lag-1), two days ago (Lag-2), and last week (Lag-7) to capture repeating cycles. |

| Cash Delivery Van Dispatch | An operational action triggering an armored security vehicle to replenish an ATM with the recommended cash amount. |

🛠️ 3. Technology Stack & Tools

VaultPulse AI strictly utilizes the mandated Big Data Analytics technology stack:

┌────────────────────────────────────────────────────────────────────────┐

│                        VAULTPULSE AI TECH STACK                        │

├───────────────────┬────────────────────────────────────────────────────┤

│ Component         │ Technologies Used                                  │

├───────────────────┼────────────────────────────────────────────────────┤

│ Containerization  │ Docker, Docker Compose (5 Isolated Microservices)  │

│ Data Streaming    │ Apache Kafka (Producer, Consumer, WebSockets)      │

│ Cluster Manager   │ Apache Zookeeper                                   │

│ Big Data Storage  │ Apache Hadoop HDFS (NameNode & DataNode Data Lake) │

│ Machine Learning  │ Scikit-Learn (Random Forest Time-Series Regressor) │

│ Backend Server    │ Python 3.10+, FastAPI, Uvicorn (ASGI)              │

│ Frontend UI       │ HTML5, Modern Vanilla CSS3 (Glassmorphism), JS     │

│ Visualizations    │ Chart.js (Historical vs. Next-Day ML Forecast)     │

│ Protocols & APIs  │ RESTful HTTP APIs + Full-Duplex WebSockets         │

└───────────────────┴────────────────────────────────────────────────────┘


Detailed Tech Stack Breakdown

1. Container Orchestration (docker-compose.yml, Dockerfile):

vaultpulse-zookeeper: Apache Zookeeper cluster coordinator on port 2181.

vaultpulse-kafka: Apache Kafka broker on ports 9092 (host) and 29092 (Docker internal network).

vaultpulse-namenode: Apache Hadoop HDFS NameNode on port 9000 (RPC) and port 9870 (Web UI).

vaultpulse-datanode: Apache Hadoop HDFS DataNode storage engine on port 9864.

vaultpulse-app: VaultPulse AI Python Application running FastAPI and the ML pipeline on port 8000.

Connected via an internal bridge network (bda-net) with persistent volume mounts.

2. Apache Kafka (kafka_pipeline/):

Topic: atm-withdrawal-transactions.

Producer: Simulates stochastic Poisson customer withdrawals across all active ATMs with realistic transaction amounts, card types, and balance decrements.

Consumer: Pulls records from Kafka, accumulates micro-batches, writes them to HDFS partitions, and broadcasts real-time events to connected UI WebSocket clients.

Dual-Mode Engine: Connects to native Kafka brokers on localhost:9092 / kafka:29092; automatically falls back to an in-memory stream buffer with zero external setup required.

3. Apache Hadoop HDFS (hdfs_storage/):

Data Lake Directory Layout: /atm_transactions/year=YYYY/month=MM/day=DD/part-XXXX.csv.

Distributed Metadata: Implements NameNode hdfs_namenode_fsimage.json metadata tracking block allocations, replication factor of 3, 128 MB standard Hadoop block size, and active DataNodes.

Supports on-demand partition creation (/api/hdfs/ingest-partition) to simulate multi-day ingestion.

4. Time-Series Machine Learning Engine (forecasting/):

Algorithm: Random Forest Regressor (n_estimators=100, dynamic bootstrapping).

Metrics Evaluated: $R^2$ Score (Accuracy %: ~83.6%), Mean Absolute Error (MAE: ~$5,183), Mean Absolute Percentage Error (MAPE: ~16.4%).

14 Engineered Features:

lag_1: Yesterday's total cash demand.

lag_2: Demand 2 days ago.

lag_7: Same-day demand last week (captures 7-day weekly seasonality).

rolling_mean_7: 7-day moving average.

rolling_std_7: 7-day demand volatility.

day_of_week_num: Integer day representation (0 = Monday, 6 = Sunday).

is_weekend: Binary flag (1 for Saturday/Sunday, 0 for weekdays).

is_holiday: Spikes for salary days (1st/2nd/month-end) and public holidays.

day_of_month: Day index (1 to 31).

atm_ATM_001 through atm_ATM_005: One-hot encoded ATM identifiers to learn unique baseline traffic for each machine.

5. Backend Web Services (dashboard/app.py):

Built on FastAPI for asynchronous high-throughput request handling.

Implements REST endpoints for telemetry, recommendations, chart feeds, manual triggers, and cash replenishment.

WebSocket endpoint (/ws/transactions) providing sub-second streaming ticker updates.

6. Interactive UI (dashboard/templates/index.html):

Premium dark-slate theme (#020617, #0f172a) with glassmorphic cards and glowing status indicators.

Responsive design with Chart.js time-series graphs, KPI metric counters, smart replenishment decision table, and a live Kafka event terminal.

📊 4. About the Dataset Used

Origin & Simulation Design

The project uses a Synthetic ATM Transaction Dataset designed in alignment with real-world Kaggle ATM withdrawal datasets and public retail-banking transaction logs:

Scale: Over 104,364 individual withdrawal transactions aggregated into 450+ daily summary records across 90+ operating days.

Realistic Behavioral Modeling: Captures payday spikes (salary disbursements on the 1st, 2nd, 30th, and 31st of each month), weekend retail surges, corporate weekday peaks, and holiday transaction multipliers.

5 Diverse ATM Profiles & Locations

The dataset models 5 distinct banking environments to stress-test the replenishment engine:

| ATM ID | ATM Name | Location Profile | Traffic Type | Base Demand | Peak Factors | Vulnerability Under Fixed Schedule |

| :--- | :--- | :--- | :--- | :--- | :--- | :--- |

| ATM_001 | Metro Central Station | Downtown Transit Hub | High-Traffic | $55,000 / day | Rush hours (8-10 AM, 5-7 PM) | Runs Dry (Stockout): $35k fixed load is depleted by 2:00 PM. |

| ATM_002 | Grand City Mall | Retail Shopping Center | High-Traffic | $48,000 / day | 1.60x weekend surge | Runs Dry (Stockout): Severe weekend cash-outs during shopping hours. |

| ATM_003 | Financial Tower Plaza | Corporate Business District | Medium-Traffic | $32,000 / day | Heavy Mon–Fri, 0.45x weekend | Optimal on weekdays, drops sharply on weekends. |

| ATM_004 | Suburban Green Park | Residential Neighborhood | Low-Traffic | $8,500 / day | Consistent low volume | Excess Cash Waste: Accumulates $40k+ in idle cash earning 0% interest. |

| ATM_005 | International Airport T2 | Airport Arrivals Gate | High-Traffic | $62,000 / day | 24/7 steady, higher txn size | Runs Dry (Stockout): High demand requires substantial daily replenishment. |

Dataset Fields & Schema

Each transaction record includes the following schema attributes:

transaction_id: Unique transaction code (e.g., TXN_PART_104364).

atm_id: Machine identifier (ATM_001 to ATM_005).

atm_name: Human-readable location name.

location: Operating environment (Transit Hub, Shopping Mall, Business District, etc.).

timestamp: High-precision timestamp (YYYY-MM-DD HH:MM:SS).

amount_withdrawn: Dispensed cash amount ($20, $40, $50, $100, $200, $300, $400, $500).

card_type: Card network (Visa, MasterCard, RuPay, American Express).

day_of_week: Day name (Monday through Sunday).

is_weekend: Binary flag (1 = Weekend, 0 = Weekday).

is_holiday: Binary flag (1 = Public/Salary Holiday, 0 = Normal Day).

remaining_cash: Live remaining cash vault balance after withdrawal.

transaction_status: Transaction result (Success / Failed).

🏗️ 5. Implementation Architecture & Data Flow

                                  VAULTPULSE AI ARCHITECTURE

   [ Customer ATM Withdrawals ]

                 │

                 ▼

   ┌───────────────────────────┐

   │    1. APACHE KAFKA        │ ──► Topic: 'atm-withdrawal-transactions'

   │    Streaming Producer     │     Real-time event buffering & balance decrements

   └─────────────┬─────────────┘

                 │

                 ▼

   ┌───────────────────────────┐

   │    2. HADOOP HDFS         │ ──► Partitioned Big Data Lake

   │    Storage Ingestion      │     /atm_transactions/year=YYYY/month=MM/day=DD/part-XXXX.csv

   │    & NameNode FSImage     │     NameNode FSImage: 3 DataNodes, 3x Replication, 128MB Blocks

   └─────────────┬─────────────┘

                 │

                 ▼

   ┌───────────────────────────┐

   │    3. TIME-SERIES ML      │ ──► Feature Engineering:

   │    Random Forest Model    │     • Lags (1, 2, 7) • 7-day Moving Avg & Std

   │    Demand Forecaster      │     • Calendar & Weekend Flags • One-Hot ATM ID

   └─────────────┬─────────────┘     Accuracy: \~84% | MAE: \~$5,100 | MAPE: \~16%

                 │

                 ▼

   ┌───────────────────────────┐

   │    4. SMART REPLENISHMENT │ ──► Dynamic 15% Safety Stock Buffer

   │    Decision Engine        │     Target Level = Forecast + Safety Stock

   │                           │     Recommended Cash = Target Level - Current Cash

   └─────────────┬─────────────┘     Alerts: 🚨 Stockout | ⚠️ Excess Cash | 🟢 Optimal

                 │

                 ▼

   ┌───────────────────────────┐

   │    5. FASTAPI & WEBSOCKET │ ──► REST APIs: /api/status, /api/recommendations, /api/chart-data

   │    Web Operations Console │     WebSocket Feed: /ws/transactions

   │                           │     Interactive Actions: Retrain, Ingest HDFS, Dispatch Cash Van

   └───────────────────────────┘


End-to-End Operational Pipeline

1. Live Transaction Ingestion (Kafka):

Customer withdrawals are pushed to Kafka topic atm-withdrawal-transactions. The producer decrements the physical vault balance of the machine in real time.

2. Distributed Lake Storage (HDFS):

The consumer batches incoming transaction streams into date partitions (year=YYYY/month=MM/day=DD/part-XXXX.csv) and updates the NameNode hdfs_namenode_fsimage.json metadata catalog.

3. Feature Engineering & ML Training:

Historical records from HDFS are loaded and enriched with 14 time-series lag and seasonal features. The Random Forest model trains over completed days, learning weekday patterns, weekend shopping spikes, and holiday effects.

4. Replenishment Optimization Math:

For tomorrow's date:

$$\text{Target Cash Level} = \text{Predicted Demand} \times 1.15 \quad (\text{Includes } 15\% \text{ Safety Buffer})$$

$$\text{Recommended Cash} = \max\Big(0, \, \min(\text{Max Capacity} - \text{Current Cash}, \, \text{Target Level} - \text{Current Cash})\Big)$$

If $\text{Current Cash} < \text{Predicted Demand} \implies$ STOCKOUT_ALERT (Red).

If $\text{Current Cash} > 2.2 \times \text{Target Level} \implies$ EXCESS_CASH_ALERT (Yellow).

Otherwise $\implies$ OPTIMAL (Green).

5. Interactive UI & Real-Time Visualization:

The browser establishes a persistent WebSocket connection to /ws/transactions. Incoming transactions update the live terminal, decrement the live cash column in the table, and trigger visual warning pulses.

💻 6. How to Run the Project

Option A: Running in Docker via CLI (Required for Evaluation & Viva)

This runs the complete 5-container Big Data cluster inside Docker using terminal commands only.

Step 1: Start the Entire Multi-Container Cluster

docker compose up --build -d


(Or double-click docker_start.bat)

Step 2: Show Running Containers to the Evaluator

docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"


Or:

docker compose ps


Step 3: Show Downloaded & Built Docker Images

docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.ID}}"


Step 4: Show Live Container CPU & RAM Stats

docker stats --no-stream


Step 5: Demonstrate Hadoop HDFS via Commands

Run these commands from PowerShell on your host machine. They use the running vaultpulse-namenode container, so you do not need to enter the container first.

# Check HDFS cluster health and DataNodes report
docker exec -it vaultpulse-namenode hdfs dfsadmin -report

# List the HDFS root directory
docker exec -it vaultpulse-namenode hdfs dfs -ls /

# List all directories and files under the project's HDFS directory
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /hdfs

# Show the ATM transaction partitions
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /hdfs/atm_transactions

# Check storage usage for the transaction dataset
docker exec -it vaultpulse-namenode hdfs dfs -du -h /hdfs/atm_transactions

Display transaction records stored in HDFS:

The following sample paths match the partitions observed in this setup. If the date folders differ on your machine, run the recursive listing command above and substitute a path that exists.

# Display the first 10 lines of the October 16, 2026 CSV file
docker exec vaultpulse-namenode hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=16/part-00000.csv | Select-Object -First 10

# Display the first 10 lines of the October 15, 2026 CSV file
docker exec vaultpulse-namenode hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=15/part-00000.csv | Select-Object -First 10

# Count records in the October 16 CSV, excluding the header
docker exec vaultpulse-namenode bash -lc "hdfs dfs -cat /hdfs/atm_transactions/year=2026/month=10/day=16/part-00000.csv | tail -n +2 | wc -l"

What to point out during the viva:

/hdfs/atm_transactions/ is the dataset's HDFS directory.

year=YYYY/month=MM/day=DD/ is the date-based partition structure.

part-00000.csv contains transaction records for that partition.

hdfs dfs -cat displays stored records; hdfs dfs -du -h shows storage usage.

HDFS Web UI: Open http://localhost:9870, then select Utilities → Browse the file system and navigate to /hdfs/atm_transactions.

Step 6: Demonstrate Apache Kafka via Commands

# List all Kafka topics

docker exec -it vaultpulse-kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe the ATM transactions topic

docker exec -it vaultpulse-kafka kafka-topics --describe --topic atm-withdrawal-transactions --bootstrap-server localhost:9092

# Stream live transactions directly into the terminal

docker exec -it vaultpulse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic atm-withdrawal-transactions --from-beginning --max-messages 5


Step 7: View Live Application Logs

docker compose logs -f app


Step 8: Open Web Interfaces in Browser

VaultPulse AI Dashboard: http://localhost:8000

Hadoop HDFS NameNode Web Console: http://localhost:9870

Hadoop HDFS DataNode Web Console: http://localhost:9864

Step 9: Stop All Containers

docker compose down


(Or double-click docker_stop.bat)

Option B: Standalone Native Python Execution (No Docker)

If you wish to run the project natively without Docker:

pip install -r requirements.txt

python run_project.py


Automatically generates the dataset, sets up local HDFS partitions, trains the model, and launches the web app at http://127.0.0.1:8000.

Option C: Step-by-Step Viva Execution (Layer-by-Layer)

Demonstrate each architectural layer sequentially:

1. Generate Dataset: python data/generate_dataset.py

2. Ingest to HDFS: python -m hdfs_storage.ingest_historical

3. Train Forecasting Model: python forecasting/train_model.py

4. Test Prediction Engine: python forecasting/predict.py

5. Launch Dashboard: python -m uvicorn dashboard.app:app --host 127.0.0.1 --port 8000

🎮 7. Dashboard Interactive Features & Button Controls

The VaultPulse AI console provides interactive controls for real-time demonstration:

┌────────────────────────────────────────────────────────────────────────────────────────────┐

│                        VAULTPULSE AI DASHBOARD NAVBAR & ACTIONS                            │

├────────────────────┬───────────────────────────────────────────────────────────────────────┤

│ Button / Control   │ What It Does (Real-Time Dynamic Action)                               │

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ 📦 Ingest HDFS     │ Advances calendar date, creates a new daily partition in HDFS on the │

│                    │ fly, and increments the active HDFS partition count (e.g. 90 ➔ 95+).  │

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ 🔄 Retrain Model   │ Re-runs ML feature extraction over all HDFS partitions, recalculates  │

│                    │ accuracy (\~84%) and MAE (\~$5,100), and live-updates the metrics badge.│

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ ⚡ Reset Balances   │ Resets all 5 ATMs back to the problem demonstration state:            │

│                    │ • ATM_001 & ATM_002: Low on cash (Immediate Stockout Risk).           │

│                    │ • ATM_004: Bloated with $45,000 (Idle Excess Cash Waste).             │

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ ⚡ Emit Txn Now    │ Injects a single high-priority withdrawal event into Kafka on demand; │

│                    │ flashes in the live terminal and decrements ATM balance on screen.    │

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ ⏸️ Pause / Resume  │ Toggles the background Kafka withdrawal transaction stream on and off.│

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ 🚚 Dispatch Van    │ Simulates sending an armored delivery van with the exact recommended  │

│ (Action Table)     │ replenishment cash, converting an ATM from STOCKOUT_ALERT to OPTIMAL. │

├────────────────────┼───────────────────────────────────────────────────────────────────────┤

│ ATM Dropdown       │ Dynamically re-renders the Chart.js graph with 14-day history and     │

│ (Chart Selector)   │ tomorrow's ML forecast curve for the selected ATM machine.            │

└────────────────────┴───────────────────────────────────────────────────────────────────────┘


📂 8. Project Directory Structure

BDA Project/

├── Dockerfile                              # Multi-stage Docker container build definition

├── docker-compose.yml                      # 5-service cluster: Zookeeper, Kafka, NameNode, DataNode, App

├── docker_start.bat                        # Windows one-click script to build & start Docker cluster

├── docker_stop.bat                         # Windows one-click script to stop all Docker containers

├── DOCKER_COMMANDS_CHEAT_SHEET.md          # Comprehensive CLI commands cheat sheet for viva presentation

├── run_project.py                          # Unified launcher for standalone execution

├── requirements.txt                        # Python package dependencies

├── README.md                               # Complete project documentation

├── data/

│   ├── generate_dataset.py                 # Generates Kaggle-style synthetic withdrawal logs

│   ├── raw_synthetic_atm_transactions.csv  # 104,364 individual transaction records

│   └── daily_atm_summary.csv               # Aggregated historical daily demand records

├── kafka_pipeline/

│   ├── kafka_manager.py                    # Dual-mode broker (Native Kafka + In-memory fallback)

│   ├── producer.py                         # Streams real-time withdrawals & tracks live balances

│   └── consumer.py                         # Batches Kafka messages to HDFS & WebSocket subscribers

├── hdfs_storage/

│   ├── hdfs_client.py                      # Hadoop HDFS partition manager & NameNode client

│   ├── ingest_historical.py                # Batch ingestion script for historical transactions

│   ├── hdfs_namenode_fsimage.json          # NameNode FSImage, block mapping & replication info

│   └── hdfs_data/                          # HDFS partitions (/year=YYYY/month=MM/day=DD/part-XXXX.csv)

├── forecasting/

│   ├── train_model.py                      # Time-series feature engineering & Random Forest training

│   ├── predict.py                          # Predictor, 15% safety stock & replenishment engine

│   └── saved_models/

│       ├── atm_forecast_model.pkl          # Trained machine learning model artifact

│       └── model_metrics.json              # Accuracy, MAE, MAPE, and R2 performance scores

└── dashboard/

    ├── app.py                              # FastAPI backend, REST endpoints & WebSocket server

    ├── templates/

    │   └── index.html                      # Glassmorphism dark-slate responsive dashboard UI

    └── static/

        ├── css/

        │   └── style.css                   # Custom styles, glow indicators & contrast rules

        └── js/

            └── app.js                      # Chart.js graphs, WebSocket handlers & button actions


🎓 9. Viva Voce & Presentation Cheat Sheet

Q1: Why is fixed-schedule replenishment bad for commercial banks?

Answer: A fixed schedule treats every machine identically. A busy downtown transit station ATM runs dry before afternoon rush hour (causing customer churn and lost interchange fees), while a quiet suburban machine sits with $40,000 in idle cash earning zero interest and racking up insurance costs.

Q2: What is the exact role of Apache Kafka in this architecture?

Answer: Kafka serves as the real-time event streaming backbone. Instead of relying on slow overnight batch dumps, customer withdrawal events are pushed immediately to the atm-withdrawal-transactions topic. This gives operations teams second-by-second visibility into remaining cash levels across the entire fleet.

Q3: How does Hadoop HDFS organize the transaction data?

Answer: HDFS organizes transactions into date-partitioned paths: /atm_transactions/year=YYYY/month=MM/day=DD/part-XXXX.csv. The NameNode tracks block allocation with a 3x replication factor. Date partitioning enables ML feature extractors to scan only the necessary time windows without performing expensive full-table scans.

Q4: Why use a Time-Series ML Model instead of a simple moving average?

Answer: A simple moving average cannot capture multi-scale seasonality, weekend retail multipliers, holiday spikes, and non-linear interactions. Our Random Forest model leverages 14 features (Lags 1, 2, 7, 7-day rolling mean and standard deviation, day-of-week, weekend flags, holiday flags, and ATM one-hot embeddings) to achieve ~84% accuracy.

Q5: How is the replenishment amount calculated?

Answer: The engine predicts tomorrow's demand and adds a 15% safety stock buffer to handle unexpected surges:

$$\text{Target Cash Level} = \text{Forecasted Demand} + 15\% \text{ Safety Stock}$$

$$\text{Recommended Cash} = \max(0, \, \text{Target Cash Level} - \text{Current Cash Balance})$$

Q6: Why run this project using Docker?

Answer: Big Data architectures like Hadoop and Kafka normally require heavy distributed infrastructure and complex environment configuration. Docker Compose encapsulates all 5 components (Zookeeper, Kafka, NameNode, DataNode, and the App) into isolated, reproducible microservices with zero host pollution and consistent networking.

Q7: How do the Docker containers communicate with each other?

Answer: They communicate over an internal bridge network (bda-net). The application accesses Kafka at kafka:29092 and the Hadoop NameNode at hdfs://namenode:9000. External clients and web browsers access the web dashboard via http://localhost:8000 and the NameNode console via http://localhost:9870.

👥 Contributors & Academic Context

Course: Big Data Analytics (BDA) Laboratory Capstone

Project Statement 47: ATM Cash Demand Forecasting & Replenishment System

Implementation: 100% Dynamic, Fully Operational, Real-Time Big Data Analytics Pipeline running in Docker.
