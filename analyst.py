import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
        host='172.16.1.29',
        user='user_kafka1',
        password='Aa@123456',
        database='analyst_db'
    )
cursor = conn.cursor()

query_max_time_per_topic = """
    SELECT topic, MAX(execution_time)
    FROM execute_time
    GROUP BY topic;
"""
cursor.execute(query_max_time_per_topic)
max_time_results = cursor.fetchall()
max_time_df = pd.DataFrame(max_time_results, columns=['topic', 'max_time'])

query_min_time_per_topic = """
    SELECT topic, MIN(execution_time)
    FROM execute_time
    GROUP BY topic;
"""
cursor.execute(query_min_time_per_topic)
min_time_results = cursor.fetchall()
min_time_df = pd.DataFrame(min_time_results, columns=['topic', 'min_time'])

query_avg_time_per_topic = """
    SELECT topic, AVG(execution_time) AS avg_time
    FROM execute_time
    GROUP BY topic;
"""
cursor.execute(query_avg_time_per_topic)
avg_time_results = cursor.fetchall()
avg_time_df = pd.DataFrame(avg_time_results, columns=['topic', 'avg_time'])

query_tp90_per_topic = """
    SELECT topic, MIN(execution_time) AS tp90
    FROM (
        SELECT topic, execution_time,
            ROW_NUMBER() OVER (PARTITION BY topic ORDER BY execution_time) as row_num,
            COUNT(*) OVER (PARTITION BY topic) as total_rows
        FROM execute_time
    ) AS ranked
    WHERE row_num >= 0.9 * total_rows
    GROUP BY topic;
"""
cursor.execute(query_tp90_per_topic)
tp90_results = cursor.fetchall()
tp90_df = pd.DataFrame(tp90_results, columns=['topic', 'tp90'])

summary_df = pd.merge(avg_time_df, tp90_df, on='topic')
summary_df = pd.merge(summary_df, max_time_df, on='topic')
summary_df = pd.merge(summary_df, min_time_df, on='topic')

summary_df.to_csv('summary_data.csv', index=False)

cursor.close()
conn.close()
