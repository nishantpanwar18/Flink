"""
Alerting System - Monitors metrics and sends alerts
Supports multiple alert channels: Console, Email, Slack, Webhook
"""

import time
import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from metrics_collector import MetricsCollector


class AlertManager:
    """Manages alerts based on metric thresholds"""
    
    def __init__(self, config=None):
        self.config = config or {}
        self.alert_history = []
        self.alert_cooldown = {}  # Prevent alert spam
        self.cooldown_period = 300  # 5 minutes
        
        # Alert thresholds
        self.thresholds = {
            'cpu_percent': 90,
            'memory_percent': 85,
            'disk_percent': 85,
            'flink_job_failed': True,
            'kafka_topic_error': True,
            'available_slots_low': 4  # Alert if less than 4 slots available
        }
    
    def check_alerts(self, metrics):
        """Check metrics against thresholds and generate alerts"""
        alerts = []
        timestamp = metrics['timestamp']
        
        # Check system metrics
        system = metrics['system']
        if system['cpu_percent'] > self.thresholds['cpu_percent']:
            alerts.append({
                'severity': 'WARNING',
                'component': 'System',
                'message': f"High CPU usage: {system['cpu_percent']:.1f}%",
                'threshold': self.thresholds['cpu_percent'],
                'current_value': system['cpu_percent']
            })
        
        if system['memory_percent'] > self.thresholds['memory_percent']:
            alerts.append({
                'severity': 'WARNING',
                'component': 'System',
                'message': f"High memory usage: {system['memory_percent']:.1f}%",
                'threshold': self.thresholds['memory_percent'],
                'current_value': system['memory_percent']
            })
        
        if system['disk_percent'] > self.thresholds['disk_percent']:
            alerts.append({
                'severity': 'WARNING',
                'component': 'System',
                'message': f"High disk usage: {system['disk_percent']:.1f}%",
                'threshold': self.thresholds['disk_percent'],
                'current_value': system['disk_percent']
            })
        
        # Check Flink metrics
        flink = metrics['flink']
        for job in flink['jobs']:
            if job['state'] != 'RUNNING':
                alerts.append({
                    'severity': 'CRITICAL',
                    'component': 'Flink',
                    'message': f"Job '{job['name']}' is {job['state']}",
                    'job_id': job['id'],
                    'state': job['state']
                })
        
        if flink['available_slots'] < self.thresholds['available_slots_low']:
            alerts.append({
                'severity': 'WARNING',
                'component': 'Flink',
                'message': f"Low available slots: {flink['available_slots']}/{flink['total_slots']}",
                'available_slots': flink['available_slots'],
                'total_slots': flink['total_slots']
            })
        
        # Check Kafka metrics
        kafka = metrics['kafka']
        for topic, info in kafka['topics'].items():
            if info.get('status') == 'error':
                alerts.append({
                    'severity': 'CRITICAL',
                    'component': 'Kafka',
                    'message': f"Topic '{topic}' has error: {info.get('error', 'Unknown')}",
                    'topic': topic
                })
        
        # Add timestamp to all alerts
        for alert in alerts:
            alert['timestamp'] = timestamp
        
        return alerts
    
    def should_send_alert(self, alert):
        """Check if alert should be sent (cooldown logic)"""
        alert_key = f"{alert['component']}:{alert['message']}"
        
        if alert_key in self.alert_cooldown:
            last_sent = self.alert_cooldown[alert_key]
            if (time.time() - last_sent) < self.cooldown_period:
                return False
        
        self.alert_cooldown[alert_key] = time.time()
        return True
    
    def send_console_alert(self, alert):
        """Print alert to console"""
        severity_colors = {
            'INFO': '\033[94m',      # Blue
            'WARNING': '\033[93m',   # Yellow
            'CRITICAL': '\033[91m'   # Red
        }
        reset_color = '\033[0m'
        
        color = severity_colors.get(alert['severity'], '')
        print(f"\n{color}{'=' * 70}")
        print(f"🚨 ALERT: {alert['severity']}")
        print(f"{'=' * 70}{reset_color}")
        print(f"Time: {alert['timestamp']}")
        print(f"Component: {alert['component']}")
        print(f"Message: {alert['message']}")
        
        if 'threshold' in alert:
            print(f"Threshold: {alert['threshold']}")
            print(f"Current: {alert['current_value']}")
        
        print(f"{color}{'=' * 70}{reset_color}\n")
    
    def send_email_alert(self, alert):
        """Send alert via email"""
        if 'email' not in self.config:
            return
        
        email_config = self.config['email']
        
        msg = MIMEMultipart()
        msg['From'] = email_config['from']
        msg['To'] = email_config['to']
        msg['Subject'] = f"[{alert['severity']}] {alert['component']}: {alert['message']}"
        
        body = f"""
Alert Details:
--------------
Severity: {alert['severity']}
Component: {alert['component']}
Message: {alert['message']}
Timestamp: {alert['timestamp']}

Threshold: {alert.get('threshold', 'N/A')}
Current Value: {alert.get('current_value', 'N/A')}

This is an automated alert from the Streaming Analytics Platform.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        try:
            server = smtplib.SMTP(email_config['smtp_host'], email_config['smtp_port'])
            server.starttls()
            server.login(email_config['username'], email_config['password'])
            server.send_message(msg)
            server.quit()
            print(f"✓ Email alert sent for: {alert['message']}")
        except Exception as e:
            print(f"✗ Failed to send email alert: {e}")
    
    def send_slack_alert(self, alert):
        """Send alert to Slack"""
        if 'slack_webhook' not in self.config:
            return
        
        color_map = {
            'INFO': '#36a64f',
            'WARNING': '#ff9900',
            'CRITICAL': '#ff0000'
        }
        
        payload = {
            'attachments': [{
                'color': color_map.get(alert['severity'], '#808080'),
                'title': f"{alert['severity']}: {alert['component']}",
                'text': alert['message'],
                'fields': [
                    {'title': 'Timestamp', 'value': alert['timestamp'], 'short': True}
                ],
                'footer': 'Streaming Analytics Platform',
                'ts': int(time.time())
            }]
        }
        
        if 'threshold' in alert:
            payload['attachments'][0]['fields'].extend([
                {'title': 'Threshold', 'value': str(alert['threshold']), 'short': True},
                {'title': 'Current', 'value': str(alert['current_value']), 'short': True}
            ])
        
        try:
            response = requests.post(
                self.config['slack_webhook'],
                json=payload,
                timeout=10
            )
            if response.status_code == 200:
                print(f"✓ Slack alert sent for: {alert['message']}")
            else:
                print(f"✗ Slack alert failed: {response.status_code}")
        except Exception as e:
            print(f"✗ Failed to send Slack alert: {e}")
    
    def send_webhook_alert(self, alert):
        """Send alert to custom webhook"""
        if 'webhook_url' not in self.config:
            return
        
        try:
            response = requests.post(
                self.config['webhook_url'],
                json=alert,
                timeout=10
            )
            if response.status_code == 200:
                print(f"✓ Webhook alert sent for: {alert['message']}")
            else:
                print(f"✗ Webhook alert failed: {response.status_code}")
        except Exception as e:
            print(f"✗ Failed to send webhook alert: {e}")
    
    def send_alert(self, alert):
        """Send alert through all configured channels"""
        if not self.should_send_alert(alert):
            return
        
        # Always send to console
        self.send_console_alert(alert)
        
        # Send to configured channels
        if 'email' in self.config:
            self.send_email_alert(alert)
        
        if 'slack_webhook' in self.config:
            self.send_slack_alert(alert)
        
        if 'webhook_url' in self.config:
            self.send_webhook_alert(alert)
        
        # Store in history
        self.alert_history.append(alert)
    
    def process_alerts(self, alerts):
        """Process and send all alerts"""
        for alert in alerts:
            self.send_alert(alert)


def run_monitoring(config_file=None):
    """Run continuous monitoring with alerting"""
    # Load config if provided
    config = {}
    if config_file:
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
    
    collector = MetricsCollector()
    alert_manager = AlertManager(config)
    
    print("=" * 70)
    print("MONITORING & ALERTING SYSTEM - RUNNING")
    print("=" * 70)
    print("Monitoring interval: 10 seconds")
    print("Alert cooldown: 5 minutes")
    print("Press Ctrl+C to stop")
    print("-" * 70)
    
    try:
        while True:
            # Collect metrics
            metrics = collector.collect_all_metrics()
            
            # Check for alerts
            alerts = alert_manager.check_alerts(metrics)
            
            # Process alerts
            if alerts:
                alert_manager.process_alerts(alerts)
            else:
                # Print status update
                summary = collector.get_metrics_summary()
                print(f"[{summary['timestamp']}] Status: {summary['overall_status']} | "
                      f"Jobs: {summary['flink_running_jobs']}/{summary['flink_jobs']} | "
                      f"CPU: {summary['cpu_usage']:.1f}% | "
                      f"Mem: {summary['memory_usage']:.1f}%")
            
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped")
        print(f"Total alerts sent: {len(alert_manager.alert_history)}")


if __name__ == '__main__':
    import sys
    
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    run_monitoring(config_file)
