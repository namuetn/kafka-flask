from confluent_kafka import Consumer, KafkaException
from multiprocessing import Process

def consume_messages(consumer_conf, topics, group_id):
    consumer_conf['group.id'] = group_id
    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)

    try:
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
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    bootstrap_servers = 'localhost:9092'
    topics = ['topic-026']
    group_id = 'my_consumer_group'

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic if no offset is stored
    }

    processes = []
    for _ in range(5):  # Create 5 consumers in the consumer group
        process = Process(target=consume_messages, args=(consumer_conf, topics, group_id))
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()

if __name__ == '__main__':
    main()
