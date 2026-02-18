"""
Real-time Analytics Dashboard
Displays live data from Flink analytics jobs
"""

from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer
import json
import threading
from collections import deque
from datetime import datetime

app = Flask(__name__)

# Store latest data for each analytics type
latest_data = {
    'revenue': deque(maxlen=50),
    'app_usage': deque(maxlen=50),
    'video': deque(maxlen=50)
}

# Lock for thread-safe access
data_lock = threading.Lock()


def consume_revenue_stats():
    """Consume purchase analytics data"""
    consumer = KafkaConsumer(
        'revenue-stats',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard-revenue'
    )
    
    for message in consumer:
        data = message.value
        data['timestamp'] = datetime.now().strftime('%H:%M:%S')
        with data_lock:
            latest_data['revenue'].append(data)


def consume_app_usage_stats():
    """Consume app usage analytics data"""
    consumer = KafkaConsumer(
        'app-usage-stats',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard-app'
    )
    
    for message in consumer:
        data = message.value
        data['timestamp'] = datetime.now().strftime('%H:%M:%S')
        with data_lock:
            latest_data['app_usage'].append(data)


def consume_video_stats():
    """Consume video analytics data"""
    consumer = KafkaConsumer(
        'video-stats',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='dashboard-video'
    )
    
    for message in consumer:
        data = message.value
        data['timestamp'] = datetime.now().strftime('%H:%M:%S')
        with data_lock:
            latest_data['video'].append(data)


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/api/revenue')
def get_revenue_data():
    """API endpoint for revenue data"""
    with data_lock:
        return jsonify(list(latest_data['revenue']))


@app.route('/api/app-usage')
def get_app_usage_data():
    """API endpoint for app usage data"""
    with data_lock:
        return jsonify(list(latest_data['app_usage']))


@app.route('/api/video')
def get_video_data():
    """API endpoint for video data"""
    with data_lock:
        return jsonify(list(latest_data['video']))


@app.route('/api/stats')
def get_stats():
    """API endpoint for overall statistics"""
    with data_lock:
        return jsonify({
            'revenue_count': len(latest_data['revenue']),
            'app_usage_count': len(latest_data['app_usage']),
            'video_count': len(latest_data['video']),
            'last_update': datetime.now().strftime('%H:%M:%S')
        })


if __name__ == '__main__':
    # Start Kafka consumers in background threads
    threading.Thread(target=consume_revenue_stats, daemon=True).start()
    threading.Thread(target=consume_app_usage_stats, daemon=True).start()
    threading.Thread(target=consume_video_stats, daemon=True).start()
    
    print("=" * 70)
    print("REAL-TIME ANALYTICS DASHBOARD")
    print("=" * 70)
    print("Starting dashboard server...")
    print("Dashboard URL: http://localhost:5000")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=5000, debug=False)
