# Quick Start Guide - High Throughput Streaming Analytics

## Current Status: Phase 3 Running at 100K events/sec ✓

### System Overview
```
Event Simulator (90K/sec)
    ↓
Kafka Topics (10 partitions each)
    ↓
Flink Jobs (3 jobs, parallelism=6)
    ↓
Output Topics
    ↓
Real-time Dashboard
```

## Running Services

### Check All Services
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Kafka UI
- URL: http://localhost:8080
- View topics, messages, consumer groups

### Flink UI
- URL: http://localhost:8081
- View running jobs, task managers, metrics

### Dashboard
- URL: http://localhost:5000
- Real-time analytics visualization

## Simulator Control

### Check Running Simulator
```bash
ps aux | grep device_event_generator | grep -v grep
```

### Stop Current Simulator
```bash
# Find process ID
ps aux | grep device_event_generator | grep -v grep

# Kill it
kill <PID>
```

### Start Phase 3 Simulator (100K events/sec) - CURRENT
```bash
python3 simulator/device_event_generator_phase3.py
```

## Flink Jobs

### View Running Jobs
```bash
docker exec flink-jobmanager flink list
```

### Cancel a Job
```bash
docker exec flink-jobmanager flink cancel <JOB_ID>
```

### Submit New Job
```bash
./submit_to_flink.sh cluster_jobs/purchase_analytics.py
```

## Kafka Topics

### List Topics
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### View Messages (Input Topics)
```bash
# App events
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic device-app-events --max-messages 5

# Video events
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic device-video-events --max-messages 5

# Purchase events
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic device-purchase-events --max-messages 5
```

### View Analytics Output
```bash
# Revenue stats
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic revenue-stats --max-messages 5

# App usage stats
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic app-usage-stats --max-messages 5

# Video stats
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic video-stats --max-messages 5
```

## Dashboard

### Start Dashboard
```bash
python3 dashboard/app.py
```

### Stop Dashboard
```bash
ps aux | grep dashboard/app.py | grep -v grep
kill <PID>
```

## Performance Metrics

### Final Results
| Phase | Rate | Improvement | Key Optimization |
|-------|------|-------------|------------------|
| Original | 23K/sec | Baseline | Basic implementation |
| Phase 1 | 47.5K/sec | 2.1x | orjson + caching |
| Phase 2 | 90K/sec | 3.9x | Multi-process (6) |
| Phase 3 | 100K/sec | 4.3x | Multi-process (8) ← CURRENT |

### Current Configuration
- Processes: 8
- Events per process: 12,500/sec
- Total throughput: 100,000 events/sec
- Kafka partitions: 10 per topic
- Flink parallelism: 6 per job
- TaskManagers: 4 (32 total slots)

## Troubleshooting

### Simulator Not Reaching Target Rate
1. Check CPU usage: `top` or `htop`
2. Check Kafka broker health: http://localhost:8080
3. Reduce number of processes or events per process
4. Check network latency to Kafka

### Flink Jobs Not Processing
1. Check Flink UI: http://localhost:8081
2. View job logs in Flink UI
3. Verify Kafka topics have messages
4. Check TaskManager availability

### Dashboard Not Showing Data
1. Verify Flink jobs are running
2. Check output topics have messages
3. Restart dashboard: `python3 dashboard/app.py`
4. Check browser console for errors

## Files Reference

### Simulator
- `simulator/device_event_generator_phase3.py` - Phase 3 (100K/sec) ← CURRENT

### Flink Jobs
- `cluster_jobs/purchase_analytics.py` - Revenue analytics
- `cluster_jobs/app_usage_analytics.py` - App usage analytics
- `cluster_jobs/video_analytics.py` - Video analytics

### Configuration
- `docker-compose.yml` - All services configuration
- `Dockerfile.flink` - Custom Flink image with Python
- `requirements.txt` - Python dependencies

### Dashboard
- `dashboard/app.py` - Flask server
- `dashboard/templates/dashboard.html` - UI

## Next Steps

The project has achieved 100K events/sec target. For further optimization:
1. Increase to 10-12 processes for 120-150K events/sec
2. Tune Kafka broker settings (increase network/IO threads)
3. Implement async I/O with aiokafka for 150-200K events/sec
4. Add multiple Kafka brokers for 200K+ events/sec
