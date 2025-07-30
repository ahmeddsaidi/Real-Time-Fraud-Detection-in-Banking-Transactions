import csv
import time
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os

# Load correct env file
if os.environ.get("RUNNING_IN_DOCKER") == "1":
    load_dotenv(dotenv_path="/app/.env.docker")  # path inside Docker container
else:
    load_dotenv(dotenv_path=".env.local")        # path on host machine

# Access env variables
KAFKA_BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD") 
PG_DB = os.getenv("POSTGRES_DB")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('C:/Users/ahmad/Downloads/Real-Time-Fraud-Detection-in-Banking-Transactions/kafka/producer/streaming_data.csv', mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('streaming-data', value=row)
        print(f"Sent: {row}")
        time.sleep(1) 
