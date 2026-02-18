"""
Metrics Collector - Monitors critical system metrics
Collects metrics from Kafka, Flink, and system resources
"""

import time
import json
import requests
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from datetime import datetime
from collections import deque
import psutil


class MetricsCollector:
    """Collects metrics from all system components"""
    
    def __init__(self, kafka_bootstrap='localhost:9092', flink_jobmanager='localhost:8081'):
        self.kafka_bootstrap = kafka_bootstrap
        self.flink_url = f"http://{flink_jobmanager}"
        self.metrics_history = deque(maxlen=100)
        
        # Initialize Kafka admin client
        try:
            self.kafka_admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        except Exception as e:
            print(f"Warning: Could not connect to Kafka: {e}")
            self.kafka_admin = None
    
    def get_kafka_metrics(self):
        """Get Kafka topic metrics"""
        metrics = {
            'topics': {},
            'total_messages': 0,
            'total_lag': 0,
            'healthy': True
        }
        
        if not self.kafka_admin:
            metrics['healthy'] = False
            return metrics
        
        try:
            # Get topic list
            topics = ['device-app-events', 'device-video-events', 'device-purchase-events',
                     'revenue-stats', 'app-usage-stats', 'video-stats']
            
            for topic in topics:
                try:
                    # Get topic metadata
                    consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=self.kafka_bootstrap,
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        consumer_timeout_ms=1000
                    )
                    
                    # Get partition info
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions:
                        metrics['topics'][topic] = {
                            'partitions': len(partitions),
                            'status': 'healthy'
                        }
                    
                    consumer.close()
                except Exception as e:
                    metrics['topics'][topic] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    metrics['healthy'] = False
        
        except Exception as e:
            print(f"Error collecting Kafka metrics: {e}")
            metrics['healthy'] = False
        
        return metrics
    
    def get_flink_metrics(self):
        """Get Flink job metrics"""
        metrics = {
            'jobs': [],
            'taskmanagers': 0,
            'available_slots': 0,
            'total_slots': 0,
            'healthy': True
        }
        
        try:
            # Get overview
            response = requests.get(f"{self.flink_url}/overview", timeout=5)
            if response.status_code == 200:
                data = response.json()
                metrics['taskmanagers'] = data.get('taskmanagers', 0)
                metrics['available_slots'] = data.get('slots-available', 0)
                metrics['total_slots'] = data.get('slots-total', 0)
            
            # Get jobs
            response = requests.get(f"{self.flink_url}/jobs", timeout=5)
            if response.status_code == 200:
                data = response.json()
                for job in data.get('jobs', []):
                    job_id = job.get('id')
                    
                    # Get job details
                    job_response = requests.get(f"{self.flink_url}/jobs/{job_id}", timeout=5)
                    if job_response.status_code == 200:
                        job_data = job_response.json()
                        
                        metrics['jobs'].append({
                            'id': job_id,
                            'name': job_data.get('name', 'Unknown'),
                            'state': job_data.get('state', 'UNKNOWN'),
                            'start_time': job_data.get('start-time', 0),
                            'duration': job_data.get('duration', 0)
                        })
                        
                        # Check if job is not running
                        if job_data.get('state') != 'RUNNING':
                            metrics['healthy'] = False
        
        except Exception as e:
            print(f"Error collecting Flink metrics: {e}")
            metrics['healthy'] = False
        
        return metrics
    
    def get_system_metrics(self):
        """Get system resource metrics"""
        metrics = {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'network_io': psutil.net_io_counters()._asdict(),
            'healthy': True
        }
        
        # Check thresholds
        if metrics['cpu_percent'] > 95:
            metrics['healthy'] = False
        if metrics['memory_percent'] > 90:
            metrics['healthy'] = False
        if metrics['disk_percent'] > 90:
            metrics['healthy'] = False
        
        return metrics
    
    def collect_all_metrics(self):
        """Collect all metrics and return combined result"""
        timestamp = datetime.utcnow().isoformat()
        
        metrics = {
            'timestamp': timestamp,
            'kafka': self.get_kafka_metrics(),
            'flink': self.get_flink_metrics(),
            'system': self.get_system_metrics(),
            'overall_healthy': True
        }
        
        # Determine overall health
        if not metrics['kafka']['healthy'] or \
           not metrics['flink']['healthy'] or \
           not metrics['system']['healthy']:
            metrics['overall_healthy'] = False
        
        # Store in history
        self.metrics_history.append(metrics)
        
        return metrics
    
    def get_metrics_summary(self):
        """Get summary of recent metrics"""
        if not self.metrics_history:
            return None
        
        latest = self.metrics_history[-1]
        
        summary = {
            'timestamp': latest['timestamp'],
            'overall_status': 'HEALTHY' if latest['overall_healthy'] else 'UNHEALTHY',
            'kafka_topics': len(latest['kafka']['topics']),
            'flink_jobs': len(latest['flink']['jobs']),
            'flink_running_jobs': sum(1 for j in latest['flink']['jobs'] if j['state'] == 'RUNNING'),
            'cpu_usage': latest['system']['cpu_percent'],
            'memory_usage': latest['system']['memory_percent'],
            'available_slots': latest['flink']['available_slots'],
            'total_slots': latest['flink']['total_slots']
        }
        
        return summary


if __name__ == '__main__':
    collector = MetricsCollector()
    
    print("=" * 70)
    print("METRICS COLLECTOR - RUNNING")
    print("=" * 70)
    print("Collecting metrics every 10 seconds...")
    print("Press Ctrl+C to stop")
    print("-" * 70)
    
    try:
        while True:
            metrics = collector.collect_all_metrics()
            summary = collector.get_metrics_summary()
            
            print(f"\n[{summary['timestamp']}]")
            print(f"Status: {summary['overall_status']}")
            print(f"Flink Jobs: {summary['flink_running_jobs']}/{summary['flink_jobs']} running")
            print(f"Kafka Topics: {summary['kafka_topics']}")
            print(f"CPU: {summary['cpu_usage']:.1f}% | Memory: {summary['memory_usage']:.1f}%")
            print(f"Flink Slots: {summary['available_slots']}/{summary['total_slots']} available")
            
            time.sleep(10)
    
    except KeyboardInterrupt:
        print("\n\nMetrics collector stopped")
