from airflow.decorators import dag, task
from datetime import datetime, timedelta
import mysql.connector


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes= 1)
}

connection = mysql.connector.connect(
    host="localhost",
    user = "admin",
    password = "1234",
    database = "health_metrics_3"
)

cursor = connection.cursor(dictionary=True)

@dag(
    dag_id = "read_health_metrics",
    start_date = datetime(2024, 12, 2),
    default_args = default_args,
    schedule_interval = "@daily",
    catchup = False
)
def read_health_metrics():

    @task()
    def read_health_metrics_task():

        reports = {}
        try:
            cursor.callproc('agg_metrics')

            for result in cursor.stored_results():
                rows = result.fetchall()
                for row in rows:
                    reports[row['user_id']] = row
        except Exception as e:
            print(e)
        finally:
            print(reports)
            cursor.close()
            connection.close()

    read_health_metrics_task = read_health_metrics_task()
read_health_metrics = read_health_metrics()

        