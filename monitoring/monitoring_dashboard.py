"""
Monitoring Dashboard - Web UI for system metrics
Real-time visualization of Kafka, Flink, and system metrics
"""

from flask import Flask, render_template, jsonify
from metrics_collector import MetricsCollector
from alerting import AlertManager
import threading
import time
from datetime import datetime

app = Flask(__name__)

# Global state
collector = MetricsCollector()
alert_manager = AlertManager()
latest_metrics = {}
latest_alerts = []
metrics_lock = threading.Lock()


def collect_metrics_background():
    """Background thread to collect metrics"""
    global latest_metrics, latest_alerts
    
    while True:
        try:
            metrics = collector.collect_all_metrics()
            alerts = alert_manager.check_alerts(metrics)
            
            with metrics_lock:
                latest_metrics = metrics
                if alerts:
                    latest_alerts.extend(alerts)
                    # Keep only last 50 alerts
                    latest_alerts = latest_alerts[-50:]
            
            time.sleep(10)
        except Exception as e:
            print(f"Error in background collector: {e}")
            time.sleep(10)


@app.route('/')
def index():
    """Main monitoring dashboard"""
    return render_template('monitoring.html')


@app.route('/api/metrics')
def get_metrics():
    """API endpoint for current metrics"""
    with metrics_lock:
        return jsonify(latest_metrics)


@app.route('/api/summary')
def get_summary():
    """API endpoint for metrics summary"""
    summary = collector.get_metrics_summary()
    return jsonify(summary)


@app.route('/api/alerts')
def get_alerts():
    """API endpoint for recent alerts"""
    with metrics_lock:
        return jsonify({
            'alerts': latest_alerts,
            'count': len(latest_alerts)
        })


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    with metrics_lock:
        if not latest_metrics:
            return jsonify({'status': 'UNKNOWN', 'message': 'No metrics collected yet'}), 503
        
        healthy = latest_metrics.get('overall_healthy', False)
        return jsonify({
            'status': 'HEALTHY' if healthy else 'UNHEALTHY',
            'timestamp': latest_metrics.get('timestamp'),
            'components': {
                'kafka': latest_metrics['kafka']['healthy'],
                'flink': latest_metrics['flink']['healthy'],
                'system': latest_metrics['system']['healthy']
            }
        }), 200 if healthy else 503


if __name__ == '__main__':
    print("=" * 70)
    print("MONITORING DASHBOARD")
    print("=" * 70)
    print("Starting metrics collector...")
    
    # Start background metrics collection
    collector_thread = threading.Thread(target=collect_metrics_background, daemon=True)
    collector_thread.start()
    
    print("Dashboard URL: http://localhost:5001")
    print("Health Check: http://localhost:5001/api/health")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=5001, debug=False)
