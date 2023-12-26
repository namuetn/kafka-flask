from kafka import KafkaConsumer
import traceback
import json
import time
import argparse
import mysql.connector 
from mysql.connector import pooling
import time


def mysql_connection(database):
    connection = mysql.connector.connect(
        host='172.16.1.29',
        user='user_kafka1',
        password='Aa@123456',
        database=database
    )

    return connection

def mysql_connection_pooling(database):
    connection_pool = pooling.MySQLConnectionPool(
        pool_name="pynative_pool",
        pool_size=5,
        pool_reset_session=True,
        host='172.16.1.29',
        user='user_kafka1',
        password='Aa@123456',
        database=database
    )

    return connection_pool.get_connection()

def query_mysql(message, topic, connection_select, connection_insert):
    with connection_select.cursor() as cursor_select:
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

    '''
        INSERT QUERY IN ANALYST_DB
    '''
    with connection_insert.cursor() as cursor_insert:
        insert_query = f"INSERT INTO execute_time(topic, name, execution_time) VALUES ('{topic}', 'User_{message}', {execution_time})"

        try:
            cursor_insert.execute(insert_query)
            connection_insert.commit()
        except:
            print(f"Error executing INSERT query: {e}")
            traceback.print_exc()  # In ra stack trace đầy đủ
            connection_insert.rollback()

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
        connection_select = mysql_connection_pooling('kafka_db')
        connection_insert = mysql_connection_pooling('analyst_db')

        for message in consumer:
            query_mysql(message=message.value, topic=args.topic, connection_select=connection_select, connection_insert=connection_insert)
    except Exception as e:
        print('Error:', e)
        traceback.print_exc()  # In ra stack trace đầy đủ
    finally:
        connection_select.close()
        connection_insert.close()
        consumer.close()

if __name__ == '__main__':
    main()
