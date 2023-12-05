from confluent_kafka import Producer, Consumer, KafkaError
import subprocess
import time

# Function to create a Kafka topic with specified partitions
def create_kafka_topic(topic_name, partitions):
    subprocess.run(["docker", "exec", "kafka-broker", "kafka-topics", "--create",
                    "--topic", topic_name, "--partitions", str(partitions), "--replication-factor", "1",
                    "--bootstrap-server", "localhost:9092"])

# Function to create Kafka consumer group
def create_kafka_consumer_group(group_id, topic_name, num_consumers):
    for i in range(num_consumers):
        subprocess.run(["docker", "exec", "kafka-broker", "kafka-consumer-groups", "--create",
                        "--group", f"{group_id}-consumer-{i}", "--topic", topic_name, "--bootstrap-server", "localhost:9092"])

# Function to produce messages to a Kafka topic
def produce_kafka_messages(producer, topic_name, num_messages):
    for i in range(num_messages):
        producer.produce(topic=topic_name, value=f"Message {i}", key=str(i))
        producer.poll(0)  # Trigger delivery reports for produced messages

# Function to consume messages from a Kafka topic
def consume_kafka_messages(consumer, group_id, topic_name, num_messages):
    messages_consumed = 0
    while messages_consumed < num_messages:
        msg = consumer.poll(1.0)  # Adjust the timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break
        print(f"{group_id} - Received message: {msg.value().decode('utf-8')}")
        messages_consumed += 1

# Kafka broker configuration
broker_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-client'
}

# Create Kafka producer
producer = Producer(broker_conf)

# Create Kafka topics and consumer groups
topics = ["topic1", "topic2", "topic3", "topic4", "topic5"]
num_partitions = 5
num_messages_per_topic = 100000

for topic in topics:
    create_kafka_topic(topic, num_partitions)
    create_kafka_consumer_group(topic, topic, 5)

# Start Kafka producers for each topic
for topic in topics:
    produce_kafka_messages(producer, topic, num_messages_per_topic)

# Start Kafka consumers for each consumer group and topic
for topic in topics:
    for i in range(5):
        consumer_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': f"{topic}-consumer-{i}",
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic])
        consume_kafka_messages(consumer, f"{topic}-consumer-{i}", topic, num_messages_per_topic)

# Close Kafka producer
producer.flush()
