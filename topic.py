from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from confluent_kafka.admin import AdminClient
import argparse


def list_topics(bootstrap_servers):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    topic_metadata = []
    list_group_object = admin_client.list_consumer_groups().result().valid

    for group in list_group_object:
        group_info = admin_client.describe_consumer_groups([group.group_id])
        group_obj = group_info[group.group_id].result().members
        if len(group_obj) == 0:
            continue
        else:
            for group in group_obj:
                for topic_partition in group.assignment.topic_partitions:
                    topic_metadata.append(topic_partition.topic)

    topic_metadata = [topic for topic in topic_metadata if topic != '__consumer_offsets']
    topic_metadata = set(topic_metadata)

    return topic_metadata

def create_topic(topic, num_partitions, bootstrap_servers):
    if topic in list_topics(bootstrap_servers):
        return True

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f'Topic "{topic}" created successfully.')
    except TopicAlreadyExistsError as e:
        pass


    return topic

def main():
    parser = argparse.ArgumentParser(description='Create a Kafka consumer group.')
    parser.add_argument('--topic', required=True, help='Name of the topic')
    parser.add_argument('--partitions', default=5, type=int, help='Number of partitions')
    parser.add_argument('--bootstrap-servers', required=True, help='Bootstrap servers')
    parser.add_argument('--output-file', default='results.txt', help='Output file path')

    args = parser.parse_args()
    create_topic(args.topic, args.partitions, args.bootstrap_servers)

if __name__ == '__main__':
    main()
