import argparse
from kafka import KafkaProducer
import json
from topic import list_topics


def send_messages(producer, topic, num_messages):
    for i in range(num_messages):
        message = i
        producer.send(topic, value=message)

    print(f'{num_messages} messages sent to {topic}')

def main():
    bootstrap_servers = 'localhost:9092'

    parser = argparse.ArgumentParser(description='Send messages to Kafka topics')
    parser.add_argument('--bootstrap-servers', default=bootstrap_servers, help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--num-messages', type=int, default=20, help='Number of messages to send to each topic')
    # parser.add_argument('--topics', nargs='+', help='List of Kafka topics to send messages to')

    args = parser.parse_args()

    # Khởi tạo producer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # Gửi message vào từng topic
    if len(list_topics(bootstrap_servers)) != 0:
        for topic in list_topics(bootstrap_servers):
            send_messages(producer, topic, args.num_messages)
    else:
        print('No topic connected')

    producer.close()

if __name__ == '__main__':
    main()
