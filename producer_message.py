from multiprocessing import Process, Manager
from confluent_kafka import Producer
import time


def produce_messages(topic, partition, num_messages, messages, bootstrap_servers):
    producer_conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_conf)

    for i in range(num_messages):
        message = f'Message {i} for {topic}'
        producer.produce(topic, key=str(i), value=message, partition=partition)

    producer.flush()
    messages[topic] = f'Produced {num_messages} messages for {topic}'

def main():
    bootstrap_servers = 'localhost:9092'
    topics = ['topic-01', 'topic-02', 'topic-03', 'topic-04', 'topic-05', 'topic-06', 'topic-07', 'topic-08', 'topic-09', 'topic-10']
    num_messages_per_topic = 100000
    num_partitions = 10  

    with Manager() as manager:
        messages = manager.dict()

        processes = []
        for topic in topics:
            for partition in range(num_partitions):
                process = Process(target=produce_messages, args=(topic, partition, num_messages_per_topic, messages, bootstrap_servers))
                processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        for topic, message in messages.items():
            print(message)

if __name__ == '__main__':
    main()
