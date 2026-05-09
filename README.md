# Real-Time Streaming Analytics Platform

A high-throughput streaming analytics platform that processes **100,000 events/second** from simulated mobile devices using Apache Flink, Kafka, and a lakehouse architecture with Apache Iceberg.

![Architecture](https://img.shields.io/badge/Architecture-Streaming-blue)
![Throughput](https://img.shields.io/badge/Throughput-100K_events%2Fsec-green)
![Stack](https://img.shields.io/badge/Stack-Flink%20|%20Kafka%20|%20Iceberg%20|%20Trino-orange)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  Event Simulator (8 processes × 12.5K events/sec = 100K/sec)        │
│       ↓                                                              │
│  Apache Kafka (3 input topics, 10 partitions each)                  │
│       ↓                                                              │
│  ┌──────────────────────┐    ┌────────────────────────────────┐     │
│  │ Apache Flink          │    │ Lakehouse (Iceberg + MinIO)     │     │
│  │ 3 analytics jobs      │    │ Long-term storage + SQL queries │     │
│  │ 10-sec tumbling windows│    │ Queryable via Trino             │     │
│  └──────────┬───────────┘    └────────────────────────────────┘     │
│             ↓                                                        │
│  Kafka Output Topics → Flask Dashboard (real-time charts)           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

- **High-throughput event generation** — 100K events/sec using multi-process Python (bypasses GIL)
- **Real-time stream processing** — Apache Flink with 10-second tumbling windows
- **3 analytics pipelines** — Purchase revenue, app usage, video engagement
- **Live dashboard** — Flask + Chart.js with 2-second auto-refresh
- **Lakehouse storage** — Apache Iceberg tables on MinIO (S3-compatible)
- **SQL analytics** — Query historical data with Trino
- **Monitoring & alerting** — System health monitoring with configurable alerts

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Message Broker | Apache Kafka 7.5 | Event streaming with 10 partitions/topic |
| Stream Processing | Apache Flink 1.18 | Real-time aggregations |
| Event Generator | Python + multiprocessing | 100K events/sec simulation |
| Serialization | orjson | 10x faster than standard json |
| Dashboard | Flask + Chart.js | Real-time visualization |
| Object Storage | MinIO | S3-compatible storage for Iceberg |
| Table Format | Apache Iceberg | ACID transactions, time travel |
| Query Engine | Trino 435 | SQL on Iceberg tables |
| Catalog | Hive Metastore | Table metadata management |
| Orchestration | Docker Compose | Local development environment |

## Applications Simulated

### Shopping
- Amazon, Flipkart, Myntra

### Video Streaming
- Amazon Prime Video, Netflix, JioHotstar

### Social Media
- Instagram, TikTok, YouTube

## Event Types

| Type | Distribution | Example |
|------|-------------|---------|
| App Usage | 50% | User opened Netflix, session duration 1800s |
| Video Playback | 35% | Sacred Games playing at 1080p, 200ms buffering |
| Purchases | 15% | iPhone 15 Pro, $1299.99, credit card |

## Quick Start

### Prerequisites

- Docker Desktop (4GB+ RAM allocated)
- Python 3.10+
- `pip install orjson kafka-python flask psutil requests`

### 1. Start Infrastructure

```bash
docker-compose up -d
```

### 2. Create Kafka Topics

```bash
for topic in device-app-events device-video-events device-purchase-events \
             revenue-stats app-usage-stats video-stats; do
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic $topic --partitions 10 --replication-factor 1 --if-not-exists
done
```

### 3. Download Flink Kafka Connector

```bash
# Download to lib/ directory
curl -o lib/flink-sql-connector-kafka-3.0.2-1.18.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar
```

### 4. Submit Flink Jobs

```bash
./submit_to_flink.sh cluster_jobs/purchase_analytics.py
./submit_to_flink.sh cluster_jobs/app_usage_analytics.py
./submit_to_flink.sh cluster_jobs/video_analytics.py
```

### 5. Start Event Simulator

```bash
python3 simulator/event_generator.py
```

### 6. Start Dashboard

```bash
python3 dashboard/app.py
```

### 7. (Optional) Start Monitoring

```bash
python3 monitoring/monitoring_dashboard.py
```

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Flink UI | http://localhost:8081 | — |
| Dashboard | http://localhost:5000 | — |
| Monitoring | http://localhost:5001 | — |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Trino UI | http://localhost:8082 | — |

## Project Structure

```
.
├── simulator/
│   └── event_generator.py          # Multi-process event simulator (100K/sec)
├── cluster_jobs/
│   ├── purchase_analytics.py       # Revenue aggregation (Flink SQL)
│   ├── app_usage_analytics.py      # App usage metrics (Flink SQL)
│   └── video_analytics.py          # Video engagement metrics (Flink SQL)
├── dashboard/
│   ├── app.py                      # Flask backend (Kafka consumer)
│   └── templates/dashboard.html    # Real-time charts (Chart.js)
├── monitoring/
│   ├── metrics_collector.py        # Kafka/Flink/system metrics
│   ├── alerting.py                 # Alert manager (email/Slack/webhook)
│   ├── monitoring_dashboard.py     # Monitoring web UI
│   └── templates/monitoring.html   # Monitoring charts
├── lakehouse/
│   ├── conf/core-site.xml          # Hadoop S3A configuration
│   └── trino/                      # Trino catalog & config
├── models/
│   └── events.py                   # Event dataclass definitions
├── lib/                            # Flink connector JARs (not in git)
├── docker-compose.yml              # All services orchestration
├── Dockerfile.flink                # Flink + Python image
├── Dockerfile.hive-metastore       # Hive Metastore + S3A support
├── submit_to_flink.sh              # Job submission helper script
└── requirements.txt                # Python dependencies
```

## Analytics Output

### Purchase Analytics (every 10 seconds)
```json
{"product_category": "Electronics", "transaction_count": 4, "total_revenue": 2999.95, "unique_buyers": 3}
```

### App Usage Analytics (every 10 seconds)
```json
{"app_name": "Netflix", "app_category": "Video Streaming", "event_count": 150, "unique_users": 89, "total_duration_seconds": 45000}
```

### Video Analytics (every 10 seconds)
```json
{"video_id": "nf_001", "video_title": "Sacred Games", "total_events": 200, "unique_viewers": 120, "play_count": 95, "avg_buffering_ms": 275.0}
```

## Performance

| Metric | Value |
|--------|-------|
| Event generation rate | 100,000 events/sec |
| Flink processing latency | 10-second windows |
| Kafka partitions | 10 per topic |
| Flink parallelism | 6 per job |
| TaskManagers | 4 (32 total slots) |
| Simulator processes | 8 (bypasses Python GIL) |

### Optimization Journey

| Phase | Rate | Key Optimization |
|-------|------|------------------|
| Baseline | 23K/sec | Single process, standard json |
| Phase 1 | 47.5K/sec | orjson + pre-cached IDs |
| Phase 2 | 90K/sec | 6 parallel processes |
| Phase 3 | 100K/sec | 8 processes, fine-tuned |

## Lakehouse (Iceberg + Trino)

Query raw events with SQL after they're stored in the lakehouse:

```sql
-- Daily active users by app
SELECT app_name, COUNT(DISTINCT user_id) as dau
FROM iceberg.lakehouse.app_events
WHERE event_date = CURRENT_DATE
GROUP BY app_name;

-- Revenue by category
SELECT product_category, SUM(price * quantity) as revenue
FROM iceberg.lakehouse.purchase_events
GROUP BY product_category;

-- Time travel - query data as it was 1 hour ago
SELECT * FROM iceberg.lakehouse.purchase_events
FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP - INTERVAL '1' HOUR);
```

## Monitoring

The monitoring system tracks:
- **System**: CPU, memory, disk usage
- **Flink**: Job status, backpressure, available slots
- **Kafka**: Topic health, consumer lag

Alerts can be sent via console, email, Slack, or custom webhook.

## Stop Everything

```bash
docker-compose down
```

To also remove stored data:
```bash
docker-compose down -v
```

## License

MIT
