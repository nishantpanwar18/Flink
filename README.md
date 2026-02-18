# High-Throughput Streaming Analytics Platform

Real-time analytics platform processing 100K events/sec using Apache Flink, Kafka, and Python.

## Overview

This project demonstrates a production-ready streaming analytics platform that:
- Generates 100,000 events per second from 10M simulated devices
- Processes events in real-time using Apache Flink
- Provides live analytics dashboard
- Handles 9 applications across Shopping, Video Streaming, and Social Media categories

## Architecture

```
Event Simulator (100K/sec)
    ↓
Kafka Topics (10 partitions each)
    ↓
Flink Jobs (3 analytics pipelines)
    ↓
Output Topics
    ↓
Real-time Dashboard
```

## Performance

- **Throughput**: 100,000 events/second sustained
- **Latency**: 10-second tumbling windows
- **Scalability**: 8 parallel processes, 4 Flink TaskManagers
- **Stability**: ±0.1% variance over extended periods

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.10+
- 8GB+ RAM
- 8+ CPU cores (recommended)

### 1. Start Services
```bash
docker-compose up -d
sleep 30  # Wait for initialization
```

### 2. Create Kafka Topics
```bash
for topic in device-app-events device-video-events device-purchase-events revenue-stats app-usage-stats video-stats; do
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --topic $topic --partitions 10 --replication-factor 1 --if-not-exists
done
```

### 3. Submit Flink Jobs
```bash
./submit_to_flink.sh cluster_jobs/purchase_analytics.py
./submit_to_flink.sh cluster_jobs/app_usage_analytics.py
./submit_to_flink.sh cluster_jobs/video_analytics.py
```

### 4. Start Event Simulator
```bash
python3 simulator/device_event_generator_phase3.py
```

### 5. Start Dashboard (Optional)
```bash
python3 dashboard/app.py
```

### 6. Start Monitoring (Optional)
```bash
# Monitoring dashboard
python3 monitoring/monitoring_dashboard.py

# Alerting system
python3 monitoring/alerting.py
```

## Access Points

- **Flink UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **Analytics Dashboard**: http://localhost:5000
- **Monitoring Dashboard**: http://localhost:5001

## Applications

### Shopping
- Amazon
- Flipkart
- Myntra

### Video Streaming/OTT
- Amazon Prime Video
- Netflix
- JioHotstar

### Social Media
- Instagram
- TikTok
- YouTube

## Event Types

### App Usage Events (50%)
- App opens, closes, backgrounding
- Session tracking
- Duration metrics

### Video Events (35%)
- Play, pause, stop, complete
- Quality settings
- Buffering metrics

### Purchase Events (15%)
- Product transactions
- Payment methods
- Revenue tracking

## Analytics

### Revenue Analytics
- Transaction counts by category
- Total revenue calculations
- Unique buyer tracking

### App Usage Analytics
- Event counts per app
- Unique user tracking
- Total usage duration

### Video Analytics
- View counts per video
- Unique viewer tracking
- Play counts and buffering metrics

## Project Structure

```
.
├── cluster_jobs/              # Flink analytics jobs
│   ├── purchase_analytics.py
│   ├── app_usage_analytics.py
│   └── video_analytics.py
├── dashboard/                 # Real-time dashboard
│   ├── app.py
│   └── templates/
│       └── dashboard.html
├── simulator/                 # Event generator
│   └── device_event_generator_phase3.py
├── models/                    # Data models
│   └── events.py
├── lib/                       # Flink connectors
│   └── flink-sql-connector-kafka-3.0.2-1.18.jar
├── docker-compose.yml         # Service orchestration
├── Dockerfile.flink           # Custom Flink image
├── requirements.txt           # Python dependencies
├── submit_to_flink.sh         # Job submission script
├── QUICK_START.md            # Detailed guide
└── PHASE3_RESULTS.md         # Performance analysis
```

## Technology Stack

- **Stream Processing**: Apache Flink 1.18
- **Message Broker**: Apache Kafka 7.5
- **Event Generation**: Python 3.10 with multiprocessing
- **Serialization**: orjson (10x faster than standard json)
- **Dashboard**: Flask + Chart.js
- **Containerization**: Docker & Docker Compose

## Performance Optimization Journey

| Phase | Rate | Processes | Key Optimization |
|-------|------|-----------|------------------|
| Original | 23K/sec | 1 | Basic implementation |
| Phase 1 | 47.5K/sec | 1 | orjson + caching |
| Phase 2 | 90K/sec | 6 | Multi-process |
| Phase 3 | 100K/sec | 8 | Optimized multi-process |

**Total Improvement**: 4.3x faster (334% gain)

## Key Optimizations

### Event Generation
- Multi-process architecture (8 processes)
- orjson for fast JSON serialization
- Pre-cached device/user IDs (1000 each)
- Event templates to reduce object creation
- Optimized Kafka producer settings

### Kafka Configuration
- 10 partitions per topic
- LZ4 compression
- Optimized batch sizes (128KB)
- Increased network/IO threads
- No acknowledgments for maximum speed

### Flink Configuration
- 4 TaskManagers with 8 slots each (32 total)
- Parallelism of 6 per job
- Processing-time windows (10 seconds)
- Kafka source/sink connectors

## Monitoring

### Critical Metrics

- **System**: CPU, memory, disk usage
- **Flink**: Job status, task slots, TaskManagers
- **Kafka**: Topic health, consumer lag, partitions

### Monitoring Tools

1. **Monitoring Dashboard** (http://localhost:5001)
   - Real-time metrics visualization
   - System resource charts
   - Flink job status
   - Alert history

2. **Alerting System**
   - Automated threshold monitoring
   - Multiple notification channels (Console, Email, Slack, Webhook)
   - 5-minute alert cooldown
   - Configurable thresholds

3. **Health Check API**
   - Endpoint: http://localhost:5001/api/health
   - Returns 200 (healthy) or 503 (unhealthy)
   - Suitable for automated monitoring tools

### Quick Start Monitoring

```bash
# Start monitoring dashboard
python3 monitoring/monitoring_dashboard.py

# Start alerting (console only)
python3 monitoring/alerting.py

# Start alerting with config
python3 monitoring/alerting.py monitoring/alert_config.json
```

See [MONITORING_GUIDE.md](MONITORING_GUIDE.md) for complete documentation.

## Management Commands

### Check Services
```bash
docker ps
docker-compose logs -f [service-name]
```

### View Flink Jobs
```bash
docker exec flink-jobmanager flink list
```

### View Kafka Messages
```bash
# Input topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic device-app-events --max-messages 5

# Output topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic revenue-stats --max-messages 5
```

### Stop Everything
```bash
# Stop simulator and dashboard
ps aux | grep -E "(device_event_generator|dashboard)" | grep -v grep | awk '{print $2}' | xargs kill

# Stop Docker services
docker-compose down
```

## Monitoring

### Simulator Metrics
- Events/sec rate (current and average)
- Total events processed
- Per-process performance

### Flink Metrics (UI)
- Job status and uptime
- Task parallelism
- Checkpoint statistics
- Backpressure indicators

### Kafka Metrics (UI)
- Topic throughput
- Consumer lag
- Partition distribution

## Troubleshooting

### Simulator Not Reaching Target Rate
1. Check CPU usage: `top` or `htop`
2. Verify Kafka broker health
3. Reduce number of processes if needed
4. Check network latency

### Flink Jobs Not Processing
1. Check Flink UI for errors
2. View TaskManager logs
3. Verify Kafka topics have messages
4. Check available task slots

### Dashboard Not Showing Data
1. Verify Flink jobs are running
2. Check output topics have messages
3. Restart dashboard
4. Check browser console for errors

## Resource Requirements

### Minimum
- 4 CPU cores
- 8GB RAM
- 10GB disk space

### Recommended
- 8+ CPU cores
- 16GB RAM
- 20GB disk space
- SSD storage

## Future Enhancements

1. **Scale to 150K+ events/sec**
   - Increase to 10-12 processes
   - Implement async I/O with aiokafka
   - Add more Kafka brokers

2. **Advanced Analytics**
   - Machine learning predictions
   - Anomaly detection
   - User behavior analysis

3. **Production Features**
   - Exactly-once processing
   - State management
   - Alerting system
   - Metrics export (Prometheus)

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## Documentation

- [README](README.md) - Main project documentation
- [Quick Start Guide](QUICK_START.md) - Detailed setup and usage
- [Phase 3 Results](PHASE3_RESULTS.md) - Performance analysis and metrics
- [Monitoring Guide](MONITORING_GUIDE.md) - Complete monitoring and alerting setup

## Support

For issues or questions, please open a GitHub issue.
