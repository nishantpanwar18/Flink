# Monitoring & Alerting System

Complete monitoring solution for the streaming analytics platform.

## Quick Start

### 1. Install Dependencies
```bash
pip install psutil requests flask
```

### 2. Start Monitoring Dashboard
```bash
python3 monitoring/monitoring_dashboard.py
```
Access at: **http://localhost:5001**

### 3. Start Alerting System
```bash
# Console alerts only
python3 monitoring/alerting.py

# With email/Slack/webhook alerts
python3 monitoring/alerting.py monitoring/alert_config.json
```

## Features

### Monitoring Dashboard (Port 5001)
- ✓ Real-time metrics (5-second refresh)
- ✓ System resource charts (CPU, memory, disk)
- ✓ Flink job status tracking
- ✓ Kafka topic health monitoring
- ✓ Alert history visualization

### Alerting System
- ✓ Automated threshold monitoring
- ✓ Multiple notification channels:
  - Console (always enabled)
  - Email (SMTP)
  - Slack (webhook)
  - Custom webhook
- ✓ Alert cooldown (5 minutes)
- ✓ Configurable thresholds

### Metrics Collector
- ✓ Kafka metrics (topics, partitions, health)
- ✓ Flink metrics (jobs, slots, TaskManagers)
- ✓ System metrics (CPU, memory, disk, network)

## Critical Metrics & Thresholds

| Metric | Threshold | Severity | Action |
|--------|-----------|----------|--------|
| CPU Usage | >90% | WARNING | Scale resources |
| Memory Usage | >85% | WARNING | Scale resources |
| Disk Usage | >85% | WARNING | Clean up data |
| Flink Job Failed | Any | CRITICAL | Restart job |
| Available Slots | <4 | WARNING | Add TaskManagers |
| Kafka Topic Error | Any | CRITICAL | Check Kafka |

## Alert Configuration

Create `alert_config.json`:

```json
{
  "email": {
    "enabled": true,
    "from": "alerts@company.com",
    "to": "team@company.com",
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "username": "your-email@gmail.com",
    "password": "your-app-password"
  },
  "slack_webhook": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
  "thresholds": {
    "cpu_percent": 90,
    "memory_percent": 85,
    "disk_percent": 85,
    "available_slots_low": 4
  }
}
```

## API Endpoints

- **Dashboard**: http://localhost:5001
- **Metrics**: http://localhost:5001/api/metrics
- **Summary**: http://localhost:5001/api/summary
- **Alerts**: http://localhost:5001/api/alerts
- **Health Check**: http://localhost:5001/api/health

## Files

- `metrics_collector.py` - Collects metrics from all components
- `alerting.py` - Alert management and notification
- `monitoring_dashboard.py` - Web dashboard
- `templates/monitoring.html` - Dashboard UI
- `alert_config.example.json` - Example configuration

## Complete Documentation

See [MONITORING_GUIDE.md](../MONITORING_GUIDE.md) for:
- Detailed setup instructions
- Alert channel configuration
- Integration with external tools (Prometheus, Grafana, PagerDuty)
- Troubleshooting guide
- API reference
- Best practices
