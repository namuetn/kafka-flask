from kafka import KafkaConsumer
from pymongo import MongoClient
import json


# Kafka consumer configuration
consumer = KafkaConsumer(
    'WorkerTopic1',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Mongo configuration
client = MongoClient('localhost', 27017)
db = client['kafka_test']
collection = db['message']

for message in consumer:
    data = message.value
    print(data)
    # collection.insert_one(data)
