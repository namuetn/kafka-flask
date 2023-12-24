from kafka import KafkaConsumer
import pandas as pd
import traceback
import json
import time
import argparse
import mysql.connector
from mysql.connector import pooling
import os
import time
import random


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

def mysql_connection(database):
    connection = pooling.MySQLConnectionPool(
        pool_name="my_pool",
        pool_size=32,
        host='172.29.49.123',
        user='kafka',
        password='password',
        database=database
    )

    return connection.get_connection()

def query_mysql(message, topic):
    # SELECT QUERY IN KAFKA_DB
    with mysql_connection('kafka_db') as connection_select:
        with connection_select.cursor() as cursor_select:
            select_query = "SELECT * FROM stress_test WHERE name = %s"
            try:
                start_time = time.time()
                cursor_select.execute(select_query, ('User_' + str(message),))
                cursor_select.fetchall()
                end_time = time.time()

                execution_time = end_time - start_time
            except Exception as e:
                print(f"Error executing SELECT query: {e}")
                traceback.print_exc()
                execution_time = None

    # INSERT QUERY IN ANALYST_DB
    with mysql_connection('analyst_db') as connection_insert:
        with connection_insert.cursor() as cursor_insert:
            insert_query = "INSERT INTO analyst(topic, name, execution_time) VALUES (%s, %s, %s)"
            try:
                cursor_insert.execute(insert_query, (topic, 'User_' + str(message), execution_time))
                connection_insert.commit()
            except Exception as e:
                print(f"Error executing INSERT query: {e}")
                traceback.print_exc()
                connection_insert.rollback()

def create_consumer(group_id, topic, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    return consumer

def main():
    parser = argparse.ArgumentParser(description='Create a Kafka consumer group.')
    parser.add_argument('--topic', required=True, help='Name of the topic')
    parser.add_argument('--bootstrap-servers', required=True, help='Bootstrap servers')
    parser.add_argument('--output-file', default='results.txt', help='Output file path')

    args = parser.parse_args()

    consumer = create_consumer(f'group-{args.topic}', args.topic, args.bootstrap_servers)
    try:
        for message in consumer:
            query_mysql(message=message.value, topic=args.topic)
            # time.sleep(random.uniform(0.1, 0.3))
    except Exception as e:
        print('Error:', e)
        traceback.print_exc()  # In ra stack trace đầy đủ
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
