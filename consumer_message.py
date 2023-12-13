from confluent_kafka import Consumer, KafkaException
from multiprocessing import Process
from topic import list_topics
import os

def consume_messages(consumer_conf, topic, group_id):
    consumer_conf['group.id'] = f'{group_id}_{topic}'
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    output_file_path = f'received_messages_from_{topic}.txt'

    try:
        with open(output_file_path, 'a', encoding='utf-8') as output_file:
            while True:
                msg = consumer.poll(1.0)  # Timeout 1 second
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        print(msg.error())
                        break
                print(f"Consumed message: {msg.value().decode('utf-8')} from topic {msg.topic()} and partition {msg.partition()}")
                output_file.write(f"{msg.value().decode('utf-8')}\n")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    bootstrap_servers = 'localhost:9092'
    topics = list_topics(bootstrap_servers)
    group_id = 'my_consumer_group'

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic if no offset is stored
    }

    processes = []
    for topic in topics[:5]:  # Only take the first 5 topics
        process = Process(target=consume_messages, args=(consumer_conf, topic, group_id))
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()

if __name__ == '__main__':
    main()
