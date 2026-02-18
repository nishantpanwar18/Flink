"""
App Usage Analytics - Cluster Mode
Output: Kafka topic 'app-usage-stats'
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    table_env.get_config().set("parallelism.default", "6")
    
    print("=" * 70)
    print("APP USAGE ANALYTICS - RUNNING")
    print("Output: Kafka topic 'app-usage-stats'")
    print("View in Kafka UI: http://localhost:8080")
    print("=" * 70)
    
    # Source with processing time
    table_env.execute_sql("""
        CREATE TABLE app_events (
            app_name STRING,
            app_category STRING,
            action STRING,
            user_id STRING,
            duration_seconds INT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'device-app-events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'app-analytics-working',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Sink to Kafka
    table_env.execute_sql("""
        CREATE TABLE app_stats_sink (
            app_name STRING,
            app_category STRING,
            event_count BIGINT,
            unique_users BIGINT,
            total_duration_seconds BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'app-usage-stats',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)
    
    # Analytics query - 10 second processing time windows
    table_env.execute_sql("""
        INSERT INTO app_stats_sink
        SELECT 
            app_name,
            app_category,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            SUM(duration_seconds) as total_duration_seconds
        FROM app_events
        GROUP BY 
            app_name,
            app_category,
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    """)


if __name__ == '__main__':
    main()
