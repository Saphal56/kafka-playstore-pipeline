import csv
import json
import logging
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO)

# Kafka configuration
KAFKA_TOPIC = 'raw_app_data'
KAFKA_SERVER = 'localhost:9092'

# Path to dataset
CSV_FILE = 'Google-Playstore-Cleaned.csv'

# Optimized Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1,  # Wait for leader acknowledgment
    max_in_flight_requests_per_connection=5,
    linger_ms=5,            # Batch small delays for better throughput
    batch_size=32768        # 32 KB per batch (increase if needed)
)

def send_batch():
    """Send the entire CSV file in fast batches"""
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for line in reader:
                producer.send(KAFKA_TOPIC, value=line)
                count += 1
                # Optional: log progress every 1000 records
                if count % 1000 == 0:
                    logging.info(f"Sent {count} records...")
            producer.flush()
        logging.info(f"✅ Sent {count} records to Kafka topic '{KAFKA_TOPIC}'")
    except Exception as e:
        logging.error(f"🚨 Error sending data: {e}")
    finally:
        producer.close()

if __name__ == '__main__':
    logging.info("🚀 Starting optimized Kafka Producer...")
    send_batch()