import time
import logging

from locust import TaskSet, User, between, task, events
import mysql.connector


TEST_QUERY = '''
        SELECT 1;
    '''

def get_secret():
    return {
        "user": 'root',
        "password": 'password',
        "host": 'localhost',
        "database": 'kafka_db'
    }

def get_user_ids_from_file(file_path):
    with open(file_path, 'r') as file:
        user_ids = [line.strip() for line in file.readlines()]
    return user_ids

def get_sample_query(user_id=1):
    query = f"SELECT * FROM stress_test WHERE name = 'User_{user_id}'"
    conn = get_secret()

    return conn, query

def execute_query(conn_info, query, num_queries=1):
    cnx = mysql.connector.connect(**conn_info)
    cursor = cnx.cursor()

    start_time = time.time()
    res = 0

    try:
        for _ in range(num_queries):
            cursor.execute(query)
            rows = cursor.fetchall()

        total_time = time.time() - start_time

        print(f"Execute {num_queries} queries in {total_time} second")
    except Exception as e:
        total_time = time.time() - start_time
        print(f"Error when execute query in {total_time} second: {e}")

    cursor.close()
    cnx.close()

    return res

class MySqlClient:
    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            res = {'response_time': 0, 'response_length': 0}
            start_time = time.time()
            try:
                res['response_length'] = execute_query(*args, **kwargs)
                res['response_time'] = int((time.time() - start_time) * 1000)
                events.request.fire(
                    request_type="mysql",
                    name=name,
                    response_time=res['response_time'],
                    response_length=res['response_length']
                )
            except Exception as e:
                res['response_time'] = int((time.time() - start_time) * 1000)
                events.request.fire(
                    request_type="mysql",
                    name=name,
                    response_time=res['response_time'],
                    response_length=0,
                    exception=e
                )
                logging.info('Lỗi {}'.format(e))
        return wrapper

class MySqlTaskSet(TaskSet):
    def on_start(self):
        self.user_ids = get_user_ids_from_file('received_messages_from_topic-001.txt')

    @task
    def execute_query(self):
        num_queries = len(self.user_ids)
        # for i in range(num_queries):
        #     user_id = i
        #     self.client.execute_query(get_sample_query(user_id)[0], get_sample_query(user_id)[1])
        self.client.execute_query(get_sample_query()[0], get_sample_query()[1])

class MySqlLocust(User):
    tasks = [MySqlTaskSet]
    wait_time = between(0.1, 1)

    # def setup(self):
    #     self.environment.runner.user_count = 100

    def __init__(self, *args, **kwargs):
        # self.environment.runner.user_count = 5
        super(MySqlLocust, self).__init__(*args, **kwargs)
        self.client = MySqlClient()
