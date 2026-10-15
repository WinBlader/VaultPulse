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

## 🔍 Phase 3: Show Running Containers & Images to the Examiner

### 1. Show All Running Containers:
```bash
docker ps
```
Or for a clean, nicely aligned table:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
Or using Docker Compose:
```bash
docker compose ps
```
**Expected Output:**
* `vaultpulse-zookeeper` (Up, port `2181`)
* `vaultpulse-kafka` (Up, ports `9092`, `29092`)
* `vaultpulse-namenode` (Up, ports `9000`, `9870`)
* `vaultpulse-datanode` (Up, port `9864`)
* `vaultpulse-app` (Up, port `8000`)

### 2. Show All Downloaded & Built Docker Images:
```bash
docker images
```
Or for a clean summary table:
```bash
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.ID}}"
```
**Expected Images:**
* `bde2020/hadoop-namenode` (Hadoop NameNode Image)
* `bde2020/hadoop-datanode` (Hadoop DataNode Image)
* `confluentinc/cp-kafka` (Apache Kafka Broker Image)
* `confluentinc/cp-zookeeper` (Apache Zookeeper Image)
* `bdaproject-app` or `vaultpulse-app` (Your custom Python Big Data app image)

### 3. Show Live CPU & Memory Utilization of All Containers:
```bash
docker stats --no-stream
```
*Shows RAM usage in MB/GB, CPU %, and network I/O per container.*

### 4. Show in Docker Desktop GUI (Visual Demonstration):
1. Open the **Docker Desktop** application on your Windows PC.
2. Click **"Containers"** on the left menu: You will see the `bdaproject` group with 5 green running dots.
3. Click **"Images"** on the left menu: You will see all 5 downloaded Hadoop, Kafka, and App images listed with their exact disk sizes.

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
