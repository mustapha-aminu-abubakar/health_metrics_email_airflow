from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from faker import Faker
import mysql.connector


fake = Faker()

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


mysql_cred = {
    "username": "admin",
    "port": 3306,
    "host": "localhost",
    "password": "1234",  
}

connection = mysql.connector.connect(
      user= mysql_cred['username'],
      password= mysql_cred['password'],
)

cursor = connection.cursor()


def generate_users(users_count):

    users = [
        {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": fake.random.randint(22,50),
            "gender": fake.random.choice(['male', 'female', 'others']),
            "email": fake.unique.email()
        }
        for i in range(users_count)
    ]
    insert_query="""
    INSERT INTO health_metrics_4.users(first_name, last_name, age, gender, email) VALUES(%s, %s, %s, %s, %s)
    """

    users_tuples=[(
        user['first_name'],
        user['last_name'],
        user['age'],
        user['gender'],
        user['email']
    ) for user in users]

    cursor.executemany(insert_query, users_tuples)

    connection.commit()
    cursor.close()
    connection.close()


def generate_metrics(users_count):

    data = [
        {
            "user_id": fake.random.randint(1, 6),
            "date_time": fake.unique.date_time_between_dates(datetime.today() - timedelta(days=3), datetime.today()),
            "heart_rate": fake.random.randint(50, 100),  # beats per minute
            "blood_oxygen": round(fake.random.uniform(95, 100), 1),  # percentage
            "steps_count": fake.random.randint(0, 20000),  # steps
            "calories_burned": round(fake.random.uniform(100, 1000), 2),  # kcal
            "sleep_duration": round(fake.random.uniform(4, 10), 2),  # hours
            "stress_level": fake.random.randint(1, 10),  # scale of 1 to 10
            "body_temperature": round(fake.random.uniform(36.0, 37.5), 1),  # Celsius
            "activity_level": fake.random.choice([0, 0.5, 1])
        }
        for _ in range(users_count * 2000)
    ]

    insert_query = """
        INSERT INTO health_metrics_4.metrics VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """            

    data_tuples = [(
                record['user_id'], 
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


with DAG(
    dag_id= "generate_synthetic_users_health_metrics",
    start_date = datetime(2024, 12, 2),
    schedule_interval='@daily',
    default_args= default_args,
    catchup= False
) as dag:
    # start_mysql = BashOperator(
    #     task_id= "start_mysql",
    #     bash_command= "sudo service mysql start"
    # )

    import_db = MySqlOperator(
        task_id= "import_db",
        mysql_conn_id='mysql_u_admin',
        sql='/workspaces/weatherman_workflow/airflow/dags/health_metrics_3.sql',    
    )


    generate_users_task = PythonOperator(
        task_id= "generate_users",
        python_callable= generate_users,
        op_kwargs= {"users_count": 5}
    )

    generate_metrics_taks = PythonOperator(
        task_id = "generate_metrics",
        python_callable= generate_metrics,
        op_kwargs= {"users_count": 5}
    )

    import_db >> [generate_users_task, generate_metrics_taks]





