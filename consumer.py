import json
import logging
import psycopg2
from kafka import KafkaConsumer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Kafka configuration
KAFKA_TOPIC = 'raw_app_data'
KAFKA_SERVER = 'localhost:9092'

# PostgreSQL configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "playstore_analytics",
    "user": "admin",
    "password": "adminpass"
}

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='playstore-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    max_poll_records=500,       # Increase batch size per poll
)

# Function to connect to PostgreSQL
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# Create table if not exists
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_ratings (
                id SERIAL PRIMARY KEY,
                app_name TEXT,
                category TEXT,
                rating FLOAT,
                installs TEXT,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()

# Batch insert function
def insert_batch(conn, records):
    with conn.cursor() as cur:
        data = [
            (r.get("App"), r.get("Category"), r.get("Rating"), r.get("Installs"))
            for r in records
        ]
        cur.executemany("""
            INSERT INTO app_ratings (app_name, category, rating, installs)
            VALUES (%s, %s, %s, %s)
        """, data)
        conn.commit()
    logging.info(f"Stored {len(data)} records")

# Main loop
def consume_messages():
    logging.info("🚀 Starting optimized Kafka consumer...")
    conn = connect_db()
    create_table(conn)

    batch = []
    BATCH_SIZE = 1000  # Tune this based on performance

    try:
        for message in consumer:
            record = message.value

            # Filter out invalid or malformed records
            if not record.get("Rating") or record["Rating"] == 'NaN':
                continue

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                insert_batch(conn, batch)
                batch.clear()

        # Insert remaining records after loop
        if batch:
            insert_batch(conn, batch)

    except KeyboardInterrupt:
        logging.info("🛑 Shutting down consumer...")
    finally:
        if batch:
            insert_batch(conn, batch)
        consumer.close()
        conn.close()

if __name__ == '__main__':
    consume_messages()