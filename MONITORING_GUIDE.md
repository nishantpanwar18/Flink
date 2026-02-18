## Monitoring & Alerting Guide

Complete guide for monitoring critical metrics and setting up alerts for the streaming analytics platform.

## Overview

The monitoring system provides:
- **Real-time metrics collection** from Kafka, Flink, and system resources
- **Automated alerting** with multiple notification channels
- **Web dashboard** for visual monitoring
- **Health checks** for automated monitoring tools

## Quick Start

### 1. Install Dependencies

```bash
pip install psutil requests flask
```

### 2. Start Monitoring Dashboard

```bash
python3 monitoring/monitoring_dashboard.py
```

Access at: http://localhost:5001

### 3. Start Alerting System

```bash
# Console alerts only
python3 monitoring/alerting.py

# With configuration file
python3 monitoring/alerting.py monitoring/alert_config.json
```

## Critical Metrics

### System Metrics

| Metric | Threshold | Severity | Description |
|--------|-----------|----------|-------------|
| CPU Usage | >90% | WARNING | High CPU utilization |
| Memory Usage | >85% | WARNING | High memory utilization |
| Disk Usage | >85% | WARNING | Low disk space |

### Flink Metrics

| Metric | Threshold | Severity | Description |
|--------|-----------|----------|-------------|
| Job State | != RUNNING | CRITICAL | Job failed or stopped |
| Available Slots | <4 | WARNING | Low task slot availability |
| TaskManagers | <4 | WARNING | TaskManager failure |

### Kafka Metrics

| Metric | Threshold | Severity | Description |
|--------|-----------|----------|-------------|
| Topic Status | ERROR | CRITICAL | Topic unavailable |
| Consumer Lag | >10000 | WARNING | Processing backlog |
| Partition Health | ERROR | CRITICAL | Partition failure |

## Monitoring Tools

### 1. Metrics Collector

Collects metrics from all system components.

```python
from monitoring.metrics_collector import MetricsCollector

collector = MetricsCollector()
metrics = collector.collect_all_metrics()
summary = collector.get_metrics_summary()
```

**Collected Metrics:**
- Kafka: Topic status, partition count
- Flink: Job status, slots, TaskManagers
- System: CPU, memory, disk, network I/O

### 2. Alerting System

Monitors metrics and sends alerts when thresholds are exceeded.

```bash
# Start with default settings
python3 monitoring/alerting.py

# Start with custom config
python3 monitoring/alerting.py monitoring/alert_config.json
```

**Alert Channels:**
- Console (always enabled)
- Email (SMTP)
- Slack (webhook)
- Custom webhook

### 3. Monitoring Dashboard

Web-based dashboard for real-time visualization.

```bash
python3 monitoring/monitoring_dashboard.py
```

**Features:**
- Real-time metric updates (5-second refresh)
- Visual charts for system resources
- Flink job status tracking
- Kafka topic health
- Alert history

**Endpoints:**
- Dashboard: http://localhost:5001
- Metrics API: http://localhost:5001/api/metrics
- Summary API: http://localhost:5001/api/summary
- Alerts API: http://localhost:5001/api/alerts
- Health Check: http://localhost:5001/api/health

## Alert Configuration

### Configuration File

Create `monitoring/alert_config.json`:

```json
{
  "email": {
    "enabled": true,
    "from": "alerts@yourcompany.com",
    "to": "team@yourcompany.com",
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "username": "your-email@gmail.com",
    "password": "your-app-password"
  },
  "slack_webhook": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
  "webhook_url": "https://your-webhook-endpoint.com/alerts",
  "thresholds": {
    "cpu_percent": 90,
    "memory_percent": 85,
    "disk_percent": 85,
    "available_slots_low": 4
  }
}
```

### Email Alerts (Gmail)

1. Enable 2-factor authentication on Gmail
2. Generate app password: https://myaccount.google.com/apppasswords
3. Update config with app password

### Slack Alerts

1. Create Slack app: https://api.slack.com/apps
2. Enable Incoming Webhooks
3. Create webhook for your channel
4. Add webhook URL to config

### Custom Webhook

Send alerts to any HTTP endpoint:

```json
{
  "webhook_url": "https://your-api.com/alerts"
}
```

Alert payload:
```json
{
  "timestamp": "2026-02-17T10:30:00",
  "severity": "CRITICAL",
  "component": "Flink",
  "message": "Job 'purchase_analytics' is FAILED",
  "job_id": "abc123",
  "state": "FAILED"
}
```

## Alert Cooldown

Alerts have a 5-minute cooldown period to prevent spam. The same alert won't be sent again within 5 minutes.

To adjust cooldown:

```python
alert_manager = AlertManager(config)
alert_manager.cooldown_period = 600  # 10 minutes
```

## Monitoring Best Practices

### 1. Set Appropriate Thresholds

Adjust thresholds based on your workload:

```json
{
  "thresholds": {
    "cpu_percent": 85,        // Lower for production
    "memory_percent": 80,     // Lower for production
    "disk_percent": 85,
    "available_slots_low": 8  // Higher for critical jobs
  }
}
```

### 2. Monitor Multiple Channels

Use multiple alert channels for redundancy:
- Console: For development
- Email: For team notifications
- Slack: For real-time team alerts
- Webhook: For integration with monitoring tools

### 3. Regular Health Checks

Set up automated health checks:

```bash
# Cron job every 5 minutes
*/5 * * * * curl -f http://localhost:5001/api/health || echo "System unhealthy"
```

### 4. Log Retention

Store metrics and alerts for analysis:

```python
# Save metrics to file
with open('metrics.log', 'a') as f:
    f.write(json.dumps(metrics) + '\n')
```

## Integration with External Tools

### Prometheus

Export metrics to Prometheus:

```python
from prometheus_client import Gauge, start_http_server

cpu_gauge = Gauge('system_cpu_percent', 'CPU usage percentage')
memory_gauge = Gauge('system_memory_percent', 'Memory usage percentage')

# Update metrics
cpu_gauge.set(metrics['system']['cpu_percent'])
memory_gauge.set(metrics['system']['memory_percent'])

# Start Prometheus server
start_http_server(8000)
```

### Grafana

1. Add Prometheus as data source
2. Import dashboard JSON
3. Configure alerts in Grafana

### PagerDuty

Send critical alerts to PagerDuty:

```python
def send_pagerduty_alert(alert):
    payload = {
        'routing_key': 'YOUR_INTEGRATION_KEY',
        'event_action': 'trigger',
        'payload': {
            'summary': alert['message'],
            'severity': alert['severity'].lower(),
            'source': alert['component']
        }
    }
    requests.post('https://events.pagerduty.com/v2/enqueue', json=payload)
```

## Troubleshooting

### Metrics Not Collecting

1. Check Kafka is running:
   ```bash
   docker ps | grep kafka
   ```

2. Check Flink is accessible:
   ```bash
   curl http://localhost:8081/overview
   ```

3. Check network connectivity:
   ```bash
   telnet localhost 9092
   telnet localhost 8081
   ```

### Alerts Not Sending

1. Check alert configuration:
   ```bash
   cat monitoring/alert_config.json
   ```

2. Test email settings:
   ```bash
   python3 -c "import smtplib; smtplib.SMTP('smtp.gmail.com', 587)"
   ```

3. Test Slack webhook:
   ```bash
   curl -X POST -H 'Content-type: application/json' \
     --data '{"text":"Test alert"}' \
     YOUR_SLACK_WEBHOOK_URL
   ```

### Dashboard Not Loading

1. Check Flask is running:
   ```bash
   ps aux | grep monitoring_dashboard
   ```

2. Check port availability:
   ```bash
   lsof -i :5001
   ```

3. Check browser console for errors

## Monitoring Checklist

Daily:
- [ ] Check dashboard for overall health
- [ ] Review alert history
- [ ] Verify all Flink jobs are RUNNING
- [ ] Check Kafka topic health

Weekly:
- [ ] Review CPU/memory trends
- [ ] Check disk space usage
- [ ] Analyze alert patterns
- [ ] Update thresholds if needed

Monthly:
- [ ] Review monitoring configuration
- [ ] Test alert channels
- [ ] Update documentation
- [ ] Archive old metrics

## Example Monitoring Workflow

### Development

```bash
# Terminal 1: Start services
docker-compose up -d

# Terminal 2: Start simulator
python3 simulator/device_event_generator_phase3.py

# Terminal 3: Start monitoring dashboard
python3 monitoring/monitoring_dashboard.py

# Terminal 4: Watch console alerts
python3 monitoring/alerting.py
```

### Production

```bash
# Start monitoring as background service
nohup python3 monitoring/alerting.py monitoring/alert_config.json > alerts.log 2>&1 &

# Start dashboard as background service
nohup python3 monitoring/monitoring_dashboard.py > dashboard.log 2>&1 &

# Set up health check cron
echo "*/5 * * * * curl -f http://localhost:5001/api/health || mail -s 'System Down' admin@company.com" | crontab -
```

## API Reference

### GET /api/metrics

Returns complete metrics snapshot.

**Response:**
```json
{
  "timestamp": "2026-02-17T10:30:00",
  "kafka": {
    "topics": {...},
    "healthy": true
  },
  "flink": {
    "jobs": [...],
    "taskmanagers": 4,
    "available_slots": 20,
    "total_slots": 32,
    "healthy": true
  },
  "system": {
    "cpu_percent": 45.2,
    "memory_percent": 62.1,
    "disk_percent": 35.8,
    "healthy": true
  },
  "overall_healthy": true
}
```

### GET /api/summary

Returns metrics summary.

**Response:**
```json
{
  "timestamp": "2026-02-17T10:30:00",
  "overall_status": "HEALTHY",
  "kafka_topics": 6,
  "flink_jobs": 3,
  "flink_running_jobs": 3,
  "cpu_usage": 45.2,
  "memory_usage": 62.1,
  "available_slots": 20,
  "total_slots": 32
}
```

### GET /api/alerts

Returns recent alerts.

**Response:**
```json
{
  "alerts": [
    {
      "timestamp": "2026-02-17T10:25:00",
      "severity": "WARNING",
      "component": "System",
      "message": "High CPU usage: 92.3%",
      "threshold": 90,
      "current_value": 92.3
    }
  ],
  "count": 1
}
```

### GET /api/health

Health check endpoint for monitoring tools.

**Response (Healthy):**
```json
{
  "status": "HEALTHY",
  "timestamp": "2026-02-17T10:30:00",
  "components": {
    "kafka": true,
    "flink": true,
    "system": true
  }
}
```

**Status Codes:**
- 200: System healthy
- 503: System unhealthy

## Support

For issues or questions about monitoring:
1. Check logs: `monitoring/*.log`
2. Review configuration: `monitoring/alert_config.json`
3. Test individual components
4. Open GitHub issue with logs
