from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from faker import Faker
import csv


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

connection = mysql.connector.connect(
    host="localhost",       
    user="admin",  
    password="1234",  
    database="health_metrics_2" 
)

fake = Faker()

cursor = connection.cursor()

@dag(
    dag_id = 'generate_synthetic_health_metrics',
    default_args= default_args,
    schedule_interval= timedelta(minutes= 15),
    start_date= datetime(2024, 12, 1)
)
def generate_synthetic_health_metrics():

    @task
    def generate_metrics():
        data = [
            {
                "id": fake.random.randint(1, 5),
                "date_time": fake.unique.date_time_between_dates(datetime.today() - timedelta(days=3), datetime.today()),
                "heart_rate": fake.random.randint(50, 100),  # beats per minute
                "blood_oxygen": round(fake.random.uniform(95, 100), 1),  # percentage
                "steps_count": fake.random.randint(0, 20000),  # steps
                "calories_burned": round(fake.random.uniform(100, 1000), 2),  # kcal
                "sleep_duration": round(fake.random.uniform(4, 10), 2),  # hours
                "stress_level": fake.random.randint(1, 10),  # scale of 1 to 10
                "body_temperature": round(fake.random.uniform(36.0, 37.5), 1),  # Celsius
                "activity_level": fake.random.choice(["low", "moderate", "high"])
            }
            for _ in range(1000)
        ]

        insert_query = """
            INSERT INTO health_metrics.metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """            

        data_tuples = [(
                    record['id'], 
                    record['date_time'], 
                    record['heart_rate'], 
                    record['blood_oxygen'], 
                    record['steps_count'], 
                    record['calories_burned'], 
                    record['sleep_duration'], 
                    record['stress_level'], 
                    record['body_temperature'], 
                    record['activity_level']) for record in data]

        cursor.executemany(insert_query, data_tuples)

        connection.commit()
        cursor.close()
        connection.close()

    generate_metrics = generate_metrics()
generate_synthetic_health_metrics = generate_synthetic_health_metrics()
