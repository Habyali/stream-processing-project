import os
import time
import psycopg2
import redis
import json
from datetime import datetime
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
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
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

#=========
# Redis Client

redis_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)

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

def create_tables(t_env):
    # Kafka source for engagement events
    t_env.execute_sql(f"""
        CREATE TABLE engagement_events_kafka (
            payload ROW<
                id BIGINT,
                content_id STRING,
                user_id STRING,
                event_type STRING,
                event_ts STRING,
                duration_ms INT,
                device STRING,
                raw_payload STRING,
                __op STRING,
                __table STRING,
                __db STRING,
                __ts_ms BIGINT
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
            id BIGINT,
            content_id STRING,
            user_id STRING,
            event_type STRING,
            event_ts STRING,
            device STRING,
            content_type STRING,
            length_seconds INT,
            duration_ms INT,
            engagement_seconds DOUBLE,
            engagement_pct DOUBLE,
            processing_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
    """)

#=========
# Redis Processing Function

def process_to_redis(content_id, content_type, engagement_pct, event_ts):
    """Process event and update Redis with sliding window analytics"""
    try:
        if not content_id or not content_type:
            return
            
        # Current minute key for sliding window
        current_time = datetime.now()
        window_key = current_time.strftime('%Y%m%d%H%M')
        
        # Update access count
        redis_client.hincrby(f"access:by_type:{window_key}", content_type, 1)
        redis_client.expire(f"access:by_type:{window_key}", 900)  # 15 min TTL
        
        # Update engagement if available
        if engagement_pct is not None and engagement_pct > 0:
            redis_client.zadd(f"engagement:by_type:{window_key}", {content_type: engagement_pct}, incr=True)
            redis_client.expire(f"engagement:by_type:{window_key}", 900)
            
        # Update aggregated stats every minute
        if int(current_time.second) < 2:  # First 2 seconds of each minute
            update_redis_stats()
            
    except Exception as e:
        print(f"Error updating Redis: {e}")

def update_redis_stats():
    """Update aggregated statistics for last 10 minutes"""
    try:
        from collections import defaultdict
        current_time = datetime.now()
        
        engagement_totals = defaultdict(float)
        access_totals = defaultdict(int)
        
        # Aggregate last 10 minutes
        for i in range(10):
            window_time = current_time.replace(second=0, microsecond=0) - timedelta(minutes=i)
            window_key = window_time.strftime('%Y%m%d%H%M')
            
            # Sum engagement scores
            engagement_data = redis_client.zrange(f"engagement:by_type:{window_key}", 0, -1, withscores=True)
            for content_type, score in engagement_data:
                engagement_totals[content_type] += score
                
            # Sum access counts  
            access_data = redis_client.hgetall(f"access:by_type:{window_key}")
            for content_type, count in access_data.items():
                access_totals[content_type] += int(count)
        
        # Store top by engagement
        redis_client.delete("stats:top_by_engagement")
        for content_type, total in engagement_totals.items():
            if access_totals[content_type] > 0:
                avg = total / access_totals[content_type]
                redis_client.zadd("stats:top_by_engagement", {content_type: avg})
                
        # Store top by access count
        redis_client.delete("stats:top_by_access")
        for content_type, count in access_totals.items():
            redis_client.zadd("stats:top_by_access", {content_type: count})
            
        redis_client.set("stats:last_update", current_time.isoformat())
        print(f"Updated Redis stats at {current_time}")
        
    except Exception as e:
        print(f"Error updating stats: {e}")

#=========
# Processing Logic

def process_engagement_stream(t_env, content_data):
    from datetime import timedelta
    
    # Register Python UDFs
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
    
    @udf(result_type=DataTypes.BOOLEAN())
    def send_to_redis(content_id, content_type, engagement_pct, event_ts):
        process_to_redis(content_id, content_type, engagement_pct, event_ts)
        return True
    
    # Register UDFs
    t_env.create_temporary_function("lookup_content_type", lookup_content_type)
    t_env.create_temporary_function("lookup_length_seconds", lookup_length_seconds)
    t_env.create_temporary_function("send_to_redis", send_to_redis)
    
    # Process with enrichment
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
    
    # Create a second query that sends to Redis
    redis_query = f"""
        SELECT 
            send_to_redis(
                payload.content_id,
                lookup_content_type(payload.content_id),
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
                END,
                payload.event_ts
            ) as redis_result
        FROM engagement_events_kafka
        WHERE payload.__op = 'r' OR payload.__op = 'c'
    """
    
    # Execute both queries
    result = t_env.sql_query(enriched_query)
    
    # Insert to print sink
    print_job = result.execute_insert("enriched_events_print")
    
    # Execute Redis processing (this runs the UDF)
    redis_result = t_env.sql_query(redis_query)
    redis_job = redis_result.execute()
    
    print("Processing enriched events and sending to Redis...")
    print("-" * 80)
    
    # Keep running
    try:
        print_job.wait()
    except KeyboardInterrupt:
        print("\nStopping jobs...")
        print_job.get_job_client().cancel()

#=========
# Main

def main():
    print("Starting Flink engagement processor...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    
    # Test Redis connection
    try:
        redis_client.ping()
        print("Redis connection successful")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return
    
    # Wait for Kafka topic to be ready
    print("Waiting for Kafka topic to be ready...")
    time.sleep(30)
    
    # Load content data
    print("Loading content data from PostgreSQL...")
    content_data = load_content_data()
    print(f"Loaded {len(content_data)} content records")
    
    # Create environment and process
    t_env = create_stream_env()
    create_tables(t_env)
    
    print("Processing engagement stream with enrichment and Redis sink...")
    process_engagement_stream(t_env, content_data)

if __name__ == "__main__":
    main()