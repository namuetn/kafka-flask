from kafka import KafkaConsumer
import pandas as pd
import traceback
import json
import time
import argparse
import mysql.connector
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
    connection = mysql.connector.connect(
        host='172.16.1.29',
        user='user_kafka1',
        password='Aa@123456',
        database=database
    )

    return connection

def query_mysql(message, topic):
    '''
        SELECT QUERY IN KAFKA_DB
    '''
    connection_select = mysql_connection('kafka_db')
    cursor_select = connection_select.cursor()

    select_query = f"SELECT * FROM stress_test WHERE name = 'User_{message}'"
    try:
        start_time = time.time()
        cursor_select.execute(select_query)
        cursor_select.fetchall()
        end_time = time.time()

        execution_time = end_time - start_time
    except Exception as e:
        print(f"Error executing SELECT query: {e}")
        traceback.print_exc()  # In ra stack trace đầy đủ
        execution_time = None
    finally:
        cursor_select.close()
        connection_select.close()

    '''
        INSERT QUERY IN ANALYST_DB
    '''
    connection_insert = mysql_connection('analyst_db')
    cursor_insert = connection_insert.cursor()

    insert_query = f"INSERT INTO execute_time(topic, name, execution_time) VALUES ('{topic}', 'User_{message}', {execution_time})"

    try:
        cursor_insert.execute(insert_query)
        connection_insert.commit()
    except:
        print(f"Error executing INSERT query: {e}")
        traceback.print_exc()  # In ra stack trace đầy đủ
        connection_insert.rollback()
    finally:
        connection_insert.close()

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
