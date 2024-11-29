from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from faker import Faker


default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

connection = mysql.connector.connect(
    host="localhost",       
    user="admin",  
    password="1234",  
    database="health_metrics" 
)

fake = Faker()

@dag(
    dag_id = 'generate_synthetic_health_metrics',
    default_args= default_args,
    schedule_interval= timedelta(minutes= 15),
    start_date= datetime(2024, 11, 29)
)
def generate_synthetic_health_metrics():

    @task
    def generte_metrics():
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        [
            {
                "id": fake.random.randint(1, 5),
                "date_time": fake.date_time_between_dates(datetime.today() - timedelta(days=3), datetime.today()),
                "heart_rate": fake.random.randint(50, 100),  # beats per minute
                "blood_oxygen": round(fake.random.uniform(95, 100), 1),  # percentage
                "steps_count": fake.random.randint(0, 20000),  # steps
                "calories_burned": round(fake.random.uniform(100, 1000), 2),  # kcal
                "sleep_duration": round(fake.random.uniform(4, 10), 2),  # hours
                "stress_level": fake.random.randint(1, 10),  # scale of 1 to 10
                "body_temperature": round(fake.random.uniform(36.0, 37.5), 1),  # Celsius
                "activity_level": fake.random.choice(["low", "moderate", "high"])
            }
            for _ in range(350)
        ]
            
    
