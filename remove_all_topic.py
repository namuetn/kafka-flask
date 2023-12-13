from kafka.admin import KafkaAdminClient

def delete_all_topics(bootstrap_servers):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )

    topic_metadata = admin_client.list_topics()
    topics_to_delete = topic_metadata

    if topics_to_delete:
        admin_client.delete_topics(topics=topics_to_delete)
        print(f"Deleted topics: {topics_to_delete}")
    else:
        print("No topics to delete.")

def main():
    bootstrap_servers = 'localhost:9092'
    delete_all_topics(bootstrap_servers)

if __name__ == '__main__':
    main()
