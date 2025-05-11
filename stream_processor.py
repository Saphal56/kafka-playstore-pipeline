# processor/stream_processor.py
from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'playstore-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for message in consumer:
    data = message.value
    data['Rating'] = float(data.get('Rating', 0)) + 1  # Just a dummy processing step
    producer.send('processed-playstore-topic', value=data)
    print(f"Processed and forwarded: {data}")
