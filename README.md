<div align="center">

# 🚦 Real-Time Traffic Data Engineering Pipeline

**An end-to-end, production-style streaming data pipeline** — Kafka → Spark Structured Streaming → Delta Lake → Power BI

[![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)](https://delta.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

![Status](https://img.shields.io/badge/status-active-success?style=flat-square)
![Architecture](https://img.shields.io/badge/architecture-medallion-blue?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-lightgrey?style=flat-square)

</div>

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#️-project-structure)
- [Pipeline Details](#️-pipeline-details)
  - [Bronze Layer](#1-bronze-layer--raw-ingestion)
  - [Silver Layer](#2-silver-layer--cleaning--validation)
  - [Gold Layer](#3-gold-layer--star-schema)
  - [BI Layer](#4-bi-layer)
- [Getting Started](#-getting-started)
- [Service Endpoints](#-service-endpoints)
- [Key Concepts Demonstrated](#-key-concepts-demonstrated)
- [Author](#-author)

---

## 📖 Overview

This project simulates a real-world **streaming lakehouse** for city traffic data. Events are generated continuously, streamed through Kafka, and processed in real time using PySpark Structured Streaming into a **Medallion Architecture (Bronze → Silver → Gold)** backed by Delta Lake — producing analytics-ready, BI-consumable datasets.

It's built to reflect how production data platforms are actually designed: fault-tolerant streaming, explicit data quality gates, watermarking for late data, star-schema modeling, and a metastore-backed SQL layer for BI tools.

---

## 📐 Architecture

```
Traffic Event Producer
        │
        ▼
   Apache Kafka (traffic-topic)
        │
        ▼
┌──────────────────────────────────────────────┐
│           Spark Structured Streaming         │
│                                              │
│   Bronze Layer  →  Silver Layer  →  Gold     │
│   (raw ingest)     (cleaned +       Layer    │
│                     validated)     (star     │
│                                    schema)   │
└──────────────────────────────────────────────┘
        │
        ▼
   Delta Lake (warehouse)
        │
        ▼
   Hive Metastore + Thrift Server
        │
        ▼
      Power BI
```

<details>
<summary><strong>🏅 Medallion Architecture — layer responsibilities</strong></summary>

| Layer | Purpose |
|-------|---------|
| **Bronze** | Raw ingestion from Kafka, schema applied, no transformation |
| **Silver** | Data quality checks, validation, deduplication, feature engineering |
| **Gold** | Star schema — fact & dimension tables, analytics-ready |

</details>

---

## 🧰 Tech Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka (KRaft mode) |
| Stream Processing | PySpark Structured Streaming |
| Storage Format | Delta Lake |
| Metadata Catalog | Hive Metastore (PostgreSQL-backed) |
| SQL Access | Spark Thrift Server |
| BI / Dashboarding | Power BI |
| Orchestration | Docker Compose |
| Monitoring | Kafka UI, Spark Master UI |

---

## 🏗️ Project Structure

```
.
├── docker-compose.yml       # Full infrastructure (Kafka, Spark, Hive Metastore, Kafka UI)
├── apps/
│   ├── traffic_bronze.py    # Kafka → Bronze Delta table
│   ├── traffic_silver.py    # Bronze → Silver (DQ, validation, dedup, features)
│   └── traffic_gold.py      # Silver → Gold (star schema: fact + dimensions)
├── warehouse/                # Delta Lake table storage (mounted volume)
├── hive-conf/
│   └── hive-site.xml
├── commands.txt              # Setup & execution commands
└── SQL.txt                   # Hive DDL + BI views
```

---

## ⚙️ Pipeline Details

### 1. Bronze Layer — Raw Ingestion
- Consumes JSON traffic events from Kafka topic `traffic-topic`
- Applies a flexible schema (`vehicle_id`, `road_id`, `city_zone`, `speed`, `congestion_level`, `weather`, `event_time`)
- Writes raw, unmodified data to Delta with a streaming checkpoint

### 2. Silver Layer — Cleaning & Validation
- **Data Quality Flags:** missing vehicle ID, missing timestamp, corrupted JSON
- **Safe Type Casting:** speed → integer, event_time → timestamp
- **Business Rules:** speed range validation (0–160), future-timestamp rejection
- **Watermarking:** 15-minute watermark for late-arriving data
- **Deduplication:** on `(vehicle_id, event_ts)`
- **Feature Engineering:** peak-hour flag, speed-band classification (Low/Medium/High)

### 3. Gold Layer — Star Schema

<details>
<summary><strong>View star schema tables</strong></summary>

| Table | Description |
|---|---|
| `dim_zone` | Zone type (Commercial / IT Hub / Transit Hub / Residential) and traffic risk level |
| `dim_road` | Road type (Highway / City Road) and speed limit |
| `fact_traffic` | Enriched fact table with date partitioning, ready for BI joins |

</details>

### 4. BI Layer
- Hive external tables registered over Delta paths
- Type-safe SQL views (`bi_fact_traffic`, `bi_dim_zone`, `bi_dim_road`) exposed via Spark Thrift Server for direct Power BI connectivity

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.x (for the Kafka producer)

<details>
<summary><strong>1️⃣ Start the infrastructure</strong></summary>

```bash
docker compose up -d
```

</details>

<details>
<summary><strong>2️⃣ Install producer dependencies</strong></summary>

```bash
pip install kafka-python faker pytz
```

</details>

<details>
<summary><strong>3️⃣ Create the Kafka topic</strong></summary>

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 --replication-factor 1
```

</details>

<details>
<summary><strong>4️⃣ Run the pipeline layers</strong></summary>

```bash
# Bronze
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_bronze.py

# Silver
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_silver.py

# Gold
docker exec -it spark-worker /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/traffic_gold.py
```

</details>

<details>
<summary><strong>5️⃣ Register tables in Hive Metastore</strong></summary>

```bash
docker exec -it spark-worker bash
mkdir -p /tmp/spark-warehouse && chmod -R 777 /tmp/spark-warehouse

/opt/spark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse
```

Then run the DDL in `SQL.txt` to create the database, external tables, and BI views.

</details>

<details>
<summary><strong>6️⃣ Start the Thrift Server (for Power BI)</strong></summary>

```bash
/opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --conf spark.sql.warehouse.dir=/opt/spark/warehouse
```

Connect Power BI to the Thrift Server endpoint on port `10000` and build dashboards on top of `bi_fact_traffic`, `bi_dim_zone`, and `bi_dim_road`.

</details>

---

## 🌐 Service Endpoints

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Kafka UI | http://localhost:8090 |
| Spark Thrift Server (SQL/Power BI) | localhost:10000 |
| Hive Metastore | thrift://localhost:9083 |

---

## 📊 Key Concepts Demonstrated

- ✅ Real-time stream ingestion with Apache Kafka
- ✅ Structured Streaming with watermarking & stateful deduplication
- ✅ Data quality validation in a streaming context
- ✅ Medallion (Bronze/Silver/Gold) lakehouse design
- ✅ Star schema modeling (fact & dimension tables)
- ✅ ACID-compliant storage with Delta Lake
- ✅ Metastore-backed SQL access for BI tools
- ✅ Fully containerized, reproducible infrastructure
