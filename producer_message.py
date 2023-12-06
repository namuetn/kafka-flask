from multiprocessing import Process, Manager
from confluent_kafka import Producer
from kafka.admin import KafkaAdminClient, NewTopic


def create_topic(topic, num_partitions, bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )

    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)


def calculate_partition(key, num_partitions):
    # Hàm tính toán partition dựa trên key
    return hash(key) % num_partitions


def produce_messages(topic, num_messages, num_partitions, messages, bootstrap_servers):
    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    for i in range(num_messages):
        message_key = i
        partition = calculate_partition(message_key, num_partitions)

        message = f'Message {i} for {topic}'
        producer.produce(topic, key=str(message_key), value=message, partition=partition)

    producer.flush()
    messages[topic] = f'Produced {num_messages} messages for {topic}'


def main():
    bootstrap_servers = 'localhost:9092'
    topics = ['topic-026']
    num_messages_per_topic = 30
    num_partitions = 5

    for topic in topics:
        create_topic(topic, num_partitions, bootstrap_servers)

    with Manager() as manager:
        messages = manager.dict()

        processes = []
        for topic in topics:
            process = Process(target=produce_messages, args=(topic, num_messages_per_topic, num_partitions, messages, bootstrap_servers))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        for topic, message in messages.items():
            print(message)

if __name__ == '__main__':
    main()
