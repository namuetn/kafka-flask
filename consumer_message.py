from kafka import KafkaConsumer
import pandas as pd
import json
import time
import argparse
import mysql.connector
import os


def write_csv(csv_file, data_csv):
    if os.path.exists(csv_file):
        with open(csv_file, 'r') as file:
            first_line = file.readline().strip()

        if first_line:  # Non-empty file with a header
            existing_df = pd.read_csv(csv_file)
            new_df = pd.DataFrame(data_csv)
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else: 
            final_df = pd.DataFrame(data_csv)
    else:
        final_df = pd.DataFrame(data_csv)

    final_df.to_csv(csv_file, index=False)

def query_mysql(message):
    connection = mysql.connector.connect(
        host='172.16.1.29',
        user='user_kafka1',
        password='Aa@123456',
        database='kafka_db'
    )
    cursor = connection.cursor()

    sql_query = f"SELECT * FROM stress_test WHERE name = 'User_{message}'"

    start_time = time.time()
    cursor.execute(sql_query)
    result = cursor.fetchall()
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Count result for User_{message}: {len(result)}")
    print(f"Execution time in 1 query: {execution_time} seconds")

    dataframe = {
        'Query': [sql_query],
        'Execution time': [execution_time]
    }

    write_csv('result.csv', dataframe)
    
    cursor.close()
    connection.close()

def create_consumer(group_id, topic, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        # enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    return consumer

def write_results_to_file(average_time, tp90_time, file_path):
    with open(file_path, 'w') as file:
        file.write(f"Average execution time: {average_time} seconds\n")
        file.write(f"tp90 execution time: {tp90_time} seconds\n")

def main():
    parser = argparse.ArgumentParser(description='Create a Kafka consumer group.')
    parser.add_argument('--topic', required=True, help='Name of the topic')
    parser.add_argument('--partitions', default=5, type=int, help='Number of partitions')
    parser.add_argument('--bootstrap-servers', required=True, help='Bootstrap servers')
    parser.add_argument('--output-file', default='results.txt', help='Output file path')

    args = parser.parse_args()

    consumer = create_consumer(f'group-{args.topic}', args.topic, args.bootstrap_servers)
    try:
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))
            query_mysql(message=message.value)
    except Exception as e:
        print('error:', e)
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
