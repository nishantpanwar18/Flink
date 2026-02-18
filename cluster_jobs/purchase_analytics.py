"""
Purchase Analytics - Simplified with Processing Time
This will show results immediately!
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    table_env.get_config().set("parallelism.default", "6")
    
    print("=" * 70)
    print("PURCHASE ANALYTICS - PROCESSING TIME WINDOWS")
    print("Output: Kafka topic 'revenue-stats'")
    print("View in Kafka UI: http://localhost:8080")
    print("=" * 70)
    
    # Source - simplified schema
    table_env.execute_sql("""
        CREATE TABLE purchase_events (
            product_category STRING,
            price DOUBLE,
            quantity INT,
            user_id STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'device-purchase-events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'purchase-working',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Sink to Kafka
    table_env.execute_sql("""
        CREATE TABLE revenue_sink (
            product_category STRING,
            transaction_count BIGINT,
            total_revenue DOUBLE,
            unique_buyers BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'revenue-stats',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)
    
    # Query - 10 second PROCESSING TIME windows (fires immediately)
    table_env.execute_sql("""
        INSERT INTO revenue_sink
        SELECT 
            product_category,
            COUNT(*) as transaction_count,
            SUM(price * quantity) as total_revenue,
            COUNT(DISTINCT user_id) as unique_buyers
        FROM purchase_events
        GROUP BY 
            product_category,
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    """)


if __name__ == '__main__':
    main()
