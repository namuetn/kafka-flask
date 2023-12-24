from kafka import KafkaConsumer, TopicPartition
import json
import argparse

def create_consumer(group_id, topic, bootstrap_servers, partitions):
    consumer = KafkaConsumer(
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Gán consumer cho các partition tương ứng
    consumer.assign([TopicPartition(topic, partition) for partition in partitions])

    return consumer

def main():
    parser = argparse.ArgumentParser(description='Create a Kafka consumer group.')
    parser.add_argument('--topic', required=True, help='Name of the topic')
    parser.add_argument('--bootstrap-servers', required=True, help='Bootstrap servers')
    parser.add_argument('--output-file', default='results.txt', help='Output file path')

    args = parser.parse_args()
    # Số partition của topic (10 trong trường hợp này)
    num_partitions = 10
    batch_size = 10
    print(345345345)
    # Tạo 10 consumer, mỗi consumer đọc từ một partition
    consumers = [create_consumer(f'group-{args.topic}', args.topic, args.bootstrap_servers, [partition])
                 for partition in range(num_partitions)]

    try:
        # Bắt đầu đọc từ các partition
        for consumer in consumers:
            for message in consumer:
                messages_batch.append(message.value)

                if len(messages_batch) == batch_size:
                    print(messages_batch)
                    # batch_insert(messages_batch, args.topic)
                    messages_batch = []

    except Exception as e:
        print('Error:', e)
    finally:
        # Đóng tất cả các consumer khi kết thúc
        for consumer in consumers:
            consumer.close()

if __name__ == '__main__':
    main()
