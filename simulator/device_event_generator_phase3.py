"""
Device Event Simulator - Phase 3 (Optional)
Target: 100K+ events/sec

Phase 3 Optimizations:
1. Increased to 8 processes (from 6)
2. 12,500 events/sec per process
3. All Phase 1 & 2 optimizations
4. Fine-tuned for 100K sustained throughput

Note: Requires sufficient CPU cores (8+)
"""

import orjson
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from multiprocessing import Process, Value, Lock
import signal
import sys


class Phase3EventSimulator:
    """Phase 3 - 100K+ events/sec with 8 processes"""
    
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        
        # Pre-cache device and user IDs
        self.device_ids = [f"device_{i:08d}" for i in random.sample(range(1, 10000000), 1000)]
        self.user_ids = [f"user_{i:08d}" for i in random.sample(range(1, 5000000), 1000)]
        self.session_ids = [f"session_{random.randint(1000000, 9999999)}" for _ in range(100)]
        self.transaction_ids = [f"txn_{random.randint(1000000000, 9999999999)}" for _ in range(100)]
        
        # Applications
        self.apps = [
            ('Amazon', 'Shopping'),
            ('Flipkart', 'Shopping'),
            ('Myntra', 'Shopping'),
            ('Amazon Prime Video', 'Video Streaming'),
            ('Netflix', 'Video Streaming'),
            ('JioHotstar', 'Video Streaming'),
            ('Instagram', 'Social Media'),
            ('TikTok', 'Social Media'),
            ('YouTube', 'Social Media'),
        ]
        
        # Videos
        self.videos = [
            ('apv_001', 'The Family Man S3', 'Series'),
            ('apv_002', 'Mirzapur S3', 'Series'),
            ('nf_001', 'Sacred Games', 'Series'),
            ('nf_002', 'Delhi Crime', 'Series'),
            ('jh_001', 'IPL 2024 Match', 'Sports'),
            ('jh_002', 'Anupama', 'TV Show'),
            ('yt_001', 'Tech Review', 'Technology'),
            ('ig_001', 'Reels: Travel', 'Reels'),
            ('tt_001', 'Dance Video', 'Entertainment'),
        ]
        
        # Products
        self.products = [
            ('amz_001', 'iPhone 15 Pro', 'Electronics', 1299.99),
            ('amz_002', 'Samsung Galaxy S24', 'Electronics', 999.99),
            ('fk_001', 'OnePlus 12', 'Electronics', 799.99),
            ('fk_002', 'Boat Earbuds', 'Electronics', 49.99),
            ('myn_001', 'Nike Running Shoes', 'Fashion', 129.99),
            ('myn_002', 'Levis Jeans', 'Fashion', 89.99),
        ]
        
        self.device_os = ['Android', 'iOS']
        self.device_models = ['iPhone 15', 'Samsung S24', 'Pixel 8', 'OnePlus 12']
        self.actions = ['opened', 'closed', 'backgrounded']
        self.video_actions = ['play', 'pause', 'stop', 'complete']
        self.qualities = ['480p', '720p', '1080p', '4K']
        self.payment_methods = ['credit_card', 'upi', 'wallet', 'cod']
    
    def generate_app_event(self):
        app_name, app_category = random.choice(self.apps)
        action = random.choice(self.actions)
        return {
            'device_id': random.choice(self.device_ids),
            'user_id': random.choice(self.user_ids),
            'app_name': app_name,
            'app_category': app_category,
            'action': action,
            'session_id': random.choice(self.session_ids),
            'duration_seconds': random.randint(10, 3600) if action == 'closed' else None,
            'timestamp': datetime.utcnow().isoformat(),
            'device_os': random.choice(self.device_os),
            'device_model': random.choice(self.device_models),
            'app_version': f"{random.randint(1, 5)}.{random.randint(0, 9)}.0"
        }
    
    def generate_video_event(self):
        video_id, video_title, video_category = random.choice(self.videos)
        return {
            'device_id': random.choice(self.device_ids),
            'user_id': random.choice(self.user_ids),
            'video_id': video_id,
            'video_title': video_title,
            'video_category': video_category,
            'action': random.choice(self.video_actions),
            'position_seconds': random.randint(0, 3600),
            'duration_seconds': random.randint(300, 7200),
            'quality': random.choice(self.qualities),
            'timestamp': datetime.utcnow().isoformat(),
            'device_os': random.choice(self.device_os),
            'buffering_time_ms': random.randint(0, 5000) if random.random() > 0.7 else None
        }
    
    def generate_purchase_event(self):
        product_id, product_name, product_category, price = random.choice(self.products)
        return {
            'device_id': random.choice(self.device_ids),
            'user_id': random.choice(self.user_ids),
            'transaction_id': random.choice(self.transaction_ids),
            'product_id': product_id,
            'product_name': product_name,
            'product_category': product_category,
            'price': price,
            'currency': 'USD',
            'quantity': random.randint(1, 3),
            'payment_method': random.choice(self.payment_methods),
            'timestamp': datetime.utcnow().isoformat(),
            'device_os': random.choice(self.device_os),
            'country': 'IN'
        }
    
    @staticmethod
    def worker_process(process_id, events_per_second, kafka_servers, shared_counter, counter_lock, stop_flag):
        """Worker process with dedicated Kafka producer"""
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: orjson.dumps(v),
            compression_type='lz4',
            batch_size=131072,
            linger_ms=50,
            buffer_memory=268435456,
            acks=0,
            max_in_flight_requests_per_connection=10,
            retries=0
        )
        
        simulator = Phase3EventSimulator(kafka_servers)
        
        event_types = ['app', 'video', 'purchase']
        event_weights = [50, 35, 15]
        
        generators = {
            'app': simulator.generate_app_event,
            'video': simulator.generate_video_event,
            'purchase': simulator.generate_purchase_event
        }
        
        topics = {
            'app': 'device-app-events',
            'video': 'device-video-events',
            'purchase': 'device-purchase-events'
        }
        
        local_count = 0
        
        try:
            while not stop_flag.value:
                batch_start = time.time()
                
                for _ in range(events_per_second):
                    event_type = random.choices(event_types, weights=event_weights)[0]
                    event = generators[event_type]()
                    
                    try:
                        producer.send(topics[event_type], value=event)
                        local_count += 1
                    except:
                        pass
                
                if local_count >= 1000:
                    with counter_lock:
                        shared_counter.value += local_count
                    local_count = 0
                
                batch_duration = time.time() - batch_start
                sleep_time = max(0, 1.0 - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            pass
        finally:
            with counter_lock:
                shared_counter.value += local_count
            producer.flush()
            producer.close()
    
    def run(self, num_processes=8, events_per_second_per_process=12500, duration_seconds=None):
        """Run Phase 3 simulator with 8 processes"""
        total_target = num_processes * events_per_second_per_process
        
        print("=" * 70)
        print("PHASE 3 SIMULATOR - 100K+ TARGET")
        print("=" * 70)
        print(f"Processes: {num_processes}")
        print(f"Events/sec per process: {events_per_second_per_process:,}")
        print(f"Total target: {total_target:,} events/sec")
        print("\nOptimizations:")
        print("  ✓ 8 parallel processes (maximum GIL bypass)")
        print("  ✓ Producer pool (8 producers)")
        print("  ✓ orjson serialization")
        print("  ✓ Pre-cached IDs and templates")
        print("-" * 70)
        
        shared_counter = Value('i', 0)
        counter_lock = Lock()
        stop_flag = Value('i', 0)
        
        processes = []
        for i in range(num_processes):
            p = Process(
                target=self.worker_process,
                args=(i, events_per_second_per_process, self.kafka_bootstrap_servers, 
                      shared_counter, counter_lock, stop_flag)
            )
            p.start()
            processes.append(p)
            print(f"Started process {i+1}/{num_processes}")
        
        print("-" * 70)
        print("All processes started. Generating events...")
        print("Press Ctrl+C to stop")
        print("-" * 70)
        
        start_time = time.time()
        last_count = 0
        last_print = start_time
        
        def signal_handler(sig, frame):
            print("\n\nStopping all processes...")
            stop_flag.value = 1
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            while True:
                time.sleep(5)
                current_time = time.time()
                elapsed = current_time - start_time
                
                with counter_lock:
                    current_count = shared_counter.value
                
                total_rate = current_count / elapsed if elapsed > 0 else 0
                interval_count = current_count - last_count
                interval_rate = interval_count / (current_time - last_print) if (current_time - last_print) > 0 else 0
                
                print(f"Total: {current_count:,} events | "
                      f"Rate: {total_rate:,.0f} events/sec | "
                      f"Last 5s: {interval_rate:,.0f} events/sec | "
                      f"Elapsed: {elapsed:.1f}s")
                
                last_count = current_count
                last_print = current_time
                
                if duration_seconds and elapsed >= duration_seconds:
                    break
                    
        except KeyboardInterrupt:
            print("\n\nStopping...")
        finally:
            stop_flag.value = 1
            
            for p in processes:
                p.join(timeout=5)
                if p.is_alive():
                    p.terminate()
            
            elapsed = time.time() - start_time
            final_count = shared_counter.value
            avg_rate = final_count / elapsed if elapsed > 0 else 0
            
            print("\n" + "=" * 70)
            print("FINAL STATISTICS")
            print("=" * 70)
            print(f"Total events: {final_count:,}")
            print(f"Duration: {elapsed:.1f}s")
            print(f"Average rate: {avg_rate:,.0f} events/sec")
            print(f"Target rate: {total_target:,} events/sec")
            print(f"Achievement: {(avg_rate/total_target*100):.1f}%")
            print("=" * 70)


if __name__ == '__main__':
    simulator = Phase3EventSimulator()
    # 8 processes × 12.5K events/sec = 100K events/sec target
    simulator.run(num_processes=8, events_per_second_per_process=12500)
