import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

#=========
# Simple test to verify Kafka connectivity

def test_kafka_read():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # Add Kafka connector
    t_env.get_config().set("pipeline.jars", 
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    
    print(f"Testing Kafka read from {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Create simple Kafka source
    t_env.execute_sql(f"""
        CREATE TABLE test_kafka (
            `payload` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'streaming.public.engagement_events',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'test-reader',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)
    
    # Try to read and count
    result = t_env.sql_query("SELECT COUNT(*) as msg_count FROM test_kafka")
    result.execute().print()

if __name__ == "__main__":
    test_kafka_read()