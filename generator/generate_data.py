import os
import time
import uuid
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

#=========
# Configuration

fake = Faker()
CONTENT_TYPES = [
    'podcast', 'newsletter', 'video', 'webinar', 'course', 'article', 
    'ebook', 'whitepaper', 'case-study', 'tutorial', 'demo', 'interview',
    'documentary', 'livestream', 'audiobook', 'blog-post'
]
EVENT_TYPES = ['play', 'pause', 'finish', 'click']
DEVICES = ['ios', 'android', 'web-chrome', 'web-safari', 'web-firefox']

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

CONTENT_ROWS = int(os.getenv('CONTENT_ROWS', '5000'))
EVENT_INTERVAL = float(os.getenv('EVENT_INTERVAL_SECONDS', '0.5'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1'))

#=========
# Database Connection

def get_db_connection():
    max_retries = 30
    retry_delay = 2
    
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"Successfully connected to database at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            return conn
        except psycopg2.OperationalError as e:
            if i < max_retries - 1:
                print(f"Database connection failed: {e}")
                print(f"Retrying in {retry_delay}s... (attempt {i+1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect after {max_retries} attempts")
                raise e

#=========
# Content Generation

def generate_content_data(num_rows):
    content_data = []
    used_slugs = set()
    
    for i in range(num_rows):
        content_type = random.choice(CONTENT_TYPES)
        
        # Generate unique slug
        slug = f"{fake.slug()}-{i}-{uuid.uuid4().hex[:8]}"
        
        length_seconds = None
        if content_type in ['podcast', 'video', 'webinar', 'course', 'tutorial', 
                        'demo', 'interview', 'documentary', 'livestream', 'audiobook']:
            length_seconds = random.randint(60, 7200)
        elif content_type in ['article', 'case-study', 'blog-post']:
            length_seconds = random.randint(300, 1800)
        
        content_data.append({
            'id': str(uuid.uuid4()),
            'slug': slug,
            'title': fake.sentence(nb_words=4),
            'content_type': content_type,
            'length_seconds': length_seconds,
            'publish_ts': fake.date_time_between(
                start_date='-2y', 
                end_date='now'
            )
        })
    
    return content_data

def insert_content_data(conn, content_data):
    with conn.cursor() as cur:
        query = """
            INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
            VALUES (%(id)s, %(slug)s, %(title)s, %(content_type)s, %(length_seconds)s, %(publish_ts)s)
            ON CONFLICT (id) DO NOTHING
        """
        execute_batch(cur, query, content_data)
    conn.commit()

#=========
# Event Generation

def generate_engagement_event(content_ids, content_info):
    content_id = random.choice(content_ids)
    event_type = random.choice(EVENT_TYPES)
    
    duration_ms = None
    if event_type in ['play', 'pause', 'finish']:
        max_duration = content_info[content_id]['length_seconds']
        if max_duration:
            duration_ms = random.randint(1000, max_duration * 1000)
    
    return {
        'content_id': content_id,
        'user_id': str(uuid.uuid4()),
        'event_type': event_type,
        'event_ts': datetime.now(),
        'duration_ms': duration_ms,
        'device': random.choice(DEVICES),
        'raw_payload': json.dumps({
            'session_id': str(uuid.uuid4()),
            'ip': fake.ipv4(),
            'user_agent': fake.user_agent()
        })
    }

def insert_engagement_events(conn, events):
    with conn.cursor() as cur:
        query = """
            INSERT INTO engagement_events 
            (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
            VALUES (%(content_id)s, %(user_id)s, %(event_type)s, %(event_ts)s, 
                    %(duration_ms)s, %(device)s, %(raw_payload)s)
        """
        execute_batch(cur, query, events)
    conn.commit()

#=========
# Main Loop

def main():
    print("Starting data generator...")
    print(f"Database config: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['database']}")
    print(f"Content rows to generate: {CONTENT_ROWS}")
    print(f"Event interval: {EVENT_INTERVAL}s")
    
    conn = get_db_connection()
    print("Connected to PostgreSQL")
    
    # Check if content already exists
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM content")
        existing_count = cur.fetchone()[0]
    
    if existing_count == 0:
        # Generate and insert content data
        print(f"Generating {CONTENT_ROWS} content rows...")
        content_data = generate_content_data(CONTENT_ROWS)
        insert_content_data(conn, content_data)
        print(f"Inserted {len(content_data)} content rows")
    else:
        print(f"Found {existing_count} existing content rows, skipping generation")
    
    # Get all content IDs and info
    with conn.cursor() as cur:
        cur.execute("SELECT id, length_seconds FROM content")
        rows = cur.fetchall()
        content_ids = [row[0] for row in rows]
        content_info = {row[0]: {'length_seconds': row[1]} for row in rows}
    
    # Generate events continuously
    print(f"Starting event generation (interval: {EVENT_INTERVAL}s, batch: {BATCH_SIZE})...")
    
    try:
        while True:
            events = []
            for _ in range(BATCH_SIZE):
                events.append(generate_engagement_event(content_ids, content_info))
            
            insert_engagement_events(conn, events)
            print(f"Inserted {len(events)} engagement events at {datetime.now()}")
            
            time.sleep(EVENT_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nShutting down generator...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()