import argparse
from kafka.admin import KafkaAdminClient, NewTopic


def list_topics(bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )

    topic_metadata = admin_client.list_topics()

    topic_metadata = [topic for topic in topic_metadata if topic != '__consumer_offsets']

    return topic_metadata

def create_topic(topic, num_partitions, bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    return topic

def main():
    parser = argparse.ArgumentParser(description='Create a Kafka topic.')
    parser.add_argument('--topic', required=True, help='Name of the topic')
    parser.add_argument('--partitions', required=True, type=int, help='Number of partitions')
    parser.add_argument('--bootstrap-servers', required=True, help='Bootstrap servers (comma-separated)')

    args = parser.parse_args()

    topic = create_topic(args.topic, args.partitions, args.bootstrap_servers)
    print(f'Topic "{topic}" created successfully.')

if __name__ == "__main__":
    main()
