"""
Video Analytics - Cluster Mode
Output: Kafka topic 'video-stats'
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    table_env.get_config().set("parallelism.default", "6")
    
    print("=" * 70)
    print("VIDEO ANALYTICS - RUNNING")
    print("Output: Kafka topic 'video-stats'")
    print("View in Kafka UI: http://localhost:8080")
    print("=" * 70)
    
    # Source with processing time
    table_env.execute_sql("""
        CREATE TABLE video_events (
            video_id STRING,
            video_title STRING,
            video_category STRING,
            action STRING,
            user_id STRING,
            quality STRING,
            buffering_time_ms INT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'device-video-events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'video-analytics-working',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Sink to Kafka
    table_env.execute_sql("""
        CREATE TABLE video_stats_sink (
            video_id STRING,
            video_title STRING,
            video_category STRING,
            total_events BIGINT,
            unique_viewers BIGINT,
            play_count BIGINT,
            avg_buffering_ms DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'video-stats',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)
    
    # Query - 10 second processing time windows
    table_env.execute_sql("""
        INSERT INTO video_stats_sink
        SELECT 
            video_id,
            video_title,
            video_category,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_viewers,
            SUM(CASE WHEN action = 'play' THEN 1 ELSE 0 END) as play_count,
            AVG(CAST(buffering_time_ms AS DOUBLE)) as avg_buffering_ms
        FROM video_events
        GROUP BY 
            video_id,
            video_title,
            video_category,
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    """)


if __name__ == '__main__':
    main()
