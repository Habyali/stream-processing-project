import os
import time
import psycopg2
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from dotenv import load_dotenv

load_dotenv()

#=========
# Configuration

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgresql')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'streaming_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'streaming_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'streaming_pass')

#=========
# Load content data into memory

def load_content_data():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    with conn.cursor() as cur:
        cur.execute("SELECT id::text, content_type, length_seconds FROM content")
        content_map = {row[0]: {'content_type': row[1], 'length_seconds': row[2]} 
                      for row in cur.fetchall()}
    
    conn.close()
    return content_map

#=========
# Flink Setup

def create_stream_env():
    config = Configuration()
    config.set_string("execution.target", "local")
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    
    t_env = StreamTableEnvironment.create(env, settings)
    
    # Add Kafka connector
    t_env.get_config().set("pipeline.jars", 
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar")
    
    return t_env

#=========
# Table Definitions

def create_tables(t_env, content_data):
    # Kafka source for engagement events
    t_env.execute_sql(f"""
        CREATE TABLE engagement_events_kafka (
            `payload` ROW<
                `id` BIGINT,
                `content_id` STRING,
                `user_id` STRING,
                `event_type` STRING,
                `event_ts` STRING,
                `duration_ms` INT,
                `device` STRING,
                `raw_payload` STRING,
                `__op` STRING,
                `__table` STRING,
                `__db` STRING,
                `__ts_ms` BIGINT
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'streaming.public.engagement_events',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-processor',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Create print sink
    t_env.execute_sql("""
        CREATE TABLE enriched_events_print (
            `id` BIGINT,
            `content_id` STRING,
            `user_id` STRING,
            `event_type` STRING,
            `event_ts` STRING,
            `device` STRING,
            `content_type` STRING,
            `length_seconds` INT,
            `duration_ms` INT,
            `engagement_seconds` DOUBLE,
            `engagement_pct` DOUBLE,
            `processing_time` TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
    """)

#=========
# Processing Logic

def process_engagement_stream(t_env, content_data):
    # Register Python UDF for content lookup
    from pyflink.table.udf import udf
    from pyflink.table import DataTypes
    
    @udf(result_type=DataTypes.STRING())
    def lookup_content_type(content_id):
        if content_id and content_id in content_data:
            return content_data[content_id].get('content_type')
        return None
    
    @udf(result_type=DataTypes.INT())
    def lookup_length_seconds(content_id):
        if content_id and content_id in content_data:
            return content_data[content_id].get('length_seconds')
        return None
    
    # Register UDFs
    t_env.create_temporary_function("lookup_content_type", lookup_content_type)
    t_env.create_temporary_function("lookup_length_seconds", lookup_length_seconds)
    
    # Process with UDF enrichment
    enriched_query = """
        SELECT 
            payload.id as id,
            payload.content_id as content_id,
            payload.user_id as user_id,
            payload.event_type as event_type,
            payload.event_ts as event_ts,
            payload.device as device,
            lookup_content_type(payload.content_id) as content_type,
            lookup_length_seconds(payload.content_id) as length_seconds,
            payload.duration_ms as duration_ms,
            CASE 
                WHEN payload.duration_ms IS NOT NULL 
                THEN CAST(payload.duration_ms AS DOUBLE) / 1000.0
                ELSE NULL 
            END AS engagement_seconds,
            CASE 
                WHEN payload.duration_ms IS NOT NULL 
                     AND lookup_length_seconds(payload.content_id) IS NOT NULL 
                     AND lookup_length_seconds(payload.content_id) > 0
                THEN ROUND(
                    CAST(payload.duration_ms AS DOUBLE) / 1000.0 / 
                    CAST(lookup_length_seconds(payload.content_id) AS DOUBLE) * 100, 
                    2
                )
                ELSE NULL
            END AS engagement_pct,
            CURRENT_TIMESTAMP AS processing_time
        FROM engagement_events_kafka
        WHERE payload.__op = 'r' OR payload.__op = 'c'
    """
    
    # Execute query
    result = t_env.sql_query(enriched_query)
    
    # Insert into print sink
    job = result.execute_insert("enriched_events_print")
    
    print(f"Job submitted with ID: {job.get_job_client().get_job_id()}")
    print("Processing enriched events...")
    print("Check the logs below for enriched events:")
    print("-" * 80)
    
    # Keep the process running
    try:
        job.wait()
    except KeyboardInterrupt:
        print("\nStopping job...")
        job.get_job_client().cancel()

#=========
# Main

def main():
    print("Starting Flink engagement processor...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    # Wait for Kafka topic to be ready
    print("Waiting for Kafka topic to be ready...")
    time.sleep(30)
    
    # Load content data
    print("Loading content data from PostgreSQL...")
    content_data = load_content_data()
    print(f"Loaded {len(content_data)} content records")
    
    # Create environment and process
    t_env = create_stream_env()
    create_tables(t_env, content_data)
    
    print("Processing engagement stream with enrichment...")
    process_engagement_stream(t_env, content_data)

if __name__ == "__main__":
    main()