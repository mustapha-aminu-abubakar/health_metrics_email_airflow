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
    database = "health_metrics"
)

@dag(
    dag_id = "read_health_metrics",
    start_date = datetime(2024, 11, 30),
    default_args = default_args,
    schedule_interval = "@daily"
)
def read_health_metrics():

    @task()
    def read_health_metrics_task():
        read_query = """
        SELECT
            id,
            DATE(date_time) as `date`,
            AVG(heart_rate) as avg_heart_rate,
            MIN(heart_rate) as max_heart_rate,
            MAX(heart_rate) as min_heart_rate,
            AVG(blood_oxygen) as avg_blood_oxygen,
            MIN(blood_oxygen) as max_blood_oxygen,
            MAX(blood_oxygen) as min_blood_oxygen,
            AVG(steps_count) as avg_steps_count,
            MIN(steps_count) as max_steps_count,
            MAX(steps_count) as min_steps_count,
            AVG(calories_burned) as avg_calories_burned,
            MIN(calories_burned) as max_calories_burned,
            MAX(calories_burned) as min_calories_burned,
            AVG(sleep_duration) as avg_sleep_duration,
            MIN(sleep_duration) as max_sleep_duration,
            MAX(sleep_duration) as min_sleep_duration,
            AVG(stress_level) as avg_stress_level,
            MIN(stress_level) as max_stress_level,
            MAX(stress_level) as min_stress_level,
            AVG(body_temperature) as avg_body_temperature,
            MIN(body_temperature) as max_body_temperature,
            MAX(body_temperature) as min_body_temperature,
            AVG(activity_level) as avg_activity_level,
            MIN(activity_level) as max_activity_level,
            MAX(activity_level) as min_activity_level,
        FROM health_metrics.metrics
        WHERE DATEDIFF(NOW(), date_time) <= 2
        GROUP BY id, date
        ORDER BY id DESC, date DESC
        """
        