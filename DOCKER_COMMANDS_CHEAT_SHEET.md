# 💻 VaultPulse AI - Docker CLI Commands Cheat Sheet
**BDA Project Statement 47: ATM Cash Demand Forecasting & Replenishment System**

Use these exact commands in your terminal to demonstrate the Big Data pipeline running inside Docker.

---

## 📌 Phase 1: Verify Docker Setup
Check that Docker and Docker Compose are active:
```bash
docker --version
docker compose version
```

---

## 🚀 Phase 2: Build & Start All 5 Docker Containers
Spins up Zookeeper, Kafka, Hadoop NameNode, DataNode, and the VaultPulse application:
```bash
docker compose up --build -d
```
*Wait ~15-30 seconds for all containers to initialize.*

---

## 🔍 Phase 3: Verify Running Services
Show the examiner all 5 containers running healthy:
```bash
docker compose ps
```
**Expected Output:**
* `vaultpulse-zookeeper` (Up, port `2181`)
* `vaultpulse-kafka` (Up, ports `9092`, `29092`)
* `vaultpulse-namenode` (Up, ports `9000`, `9870`)
* `vaultpulse-datanode` (Up, port `9864`)
* `vaultpulse-app` (Up, port `8000`)

---

## 🐘 Phase 4: Demonstrate Hadoop HDFS via CLI
Execute native Hadoop commands inside the NameNode container:

### 1. View HDFS Cluster Health & DataNodes Report:
```bash
docker exec -it vaultpulse-namenode hdfs dfsadmin -report
```
*Shows live DataNode status, configured capacity, and block health.*

### 2. List HDFS Filesystem Contents:
```bash
docker exec -it vaultpulse-namenode hdfs dfs -ls /
```

### 3. Create or Inspect HDFS ATM Directory:
```bash
docker exec -it vaultpulse-namenode hdfs dfs -mkdir -p /atm_transactions
docker exec -it vaultpulse-namenode hdfs dfs -ls -R /
```

---

## ⚡ Phase 5: Demonstrate Apache Kafka via CLI
Execute native Kafka commands inside the Kafka container:

### 1. List Kafka Topics:
```bash
docker exec -it vaultpulse-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 2. Inspect the ATM Topic Partitions & Replication:
```bash
docker exec -it vaultpulse-kafka kafka-topics --describe --topic atm-withdrawal-transactions --bootstrap-server localhost:9092
```

### 3. Stream Live Transactions Directly in Terminal:
```bash
docker exec -it vaultpulse-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic atm-withdrawal-transactions --from-beginning --max-messages 5
```

---

## 📊 Phase 6: Live Application Logs & Telemetry
View the streaming producer, consumer, ML predictions, and WebSocket events live:
```bash
docker compose logs -f app
```
*(Press `Ctrl + C` to stop watching logs)*

---

## 🌐 Phase 7: Web Demonstration Endpoints
Open these in your web browser:
* **VaultPulse AI Operations Dashboard:** [http://localhost:8000](http://localhost:8000)
* **Official Hadoop HDFS NameNode Web UI:** [http://localhost:9870](http://localhost:9870)
* **Hadoop HDFS DataNode Web UI:** [http://localhost:9864](http://localhost:9864)

---

## 🛑 Phase 8: Stop All Services
Tears down all containers cleanly:
```bash
docker compose down
```
