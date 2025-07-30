from kafka import KafkaConsumer
import json

# Create a Kafka consumer
consumer = KafkaConsumer(
    'streaming-data',  # Topic name
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',   # Start from the beginning of the topic
    enable_auto_commit=True,
    group_id='test-consumer-group',  # Consumer group name
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started. Waiting for messages...\n")

for message in consumer:
    print(f"Received: {message.value}")
