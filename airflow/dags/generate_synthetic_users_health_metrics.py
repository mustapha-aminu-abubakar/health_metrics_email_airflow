from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from faker import Faker
import mysql.connector


fake = Faker()

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


mysql_cred = {
    "username": "admin",
    "port": 3306,
    "host": "localhost",
    "password": "1234",  
    "database": "health_metrics_3"
}

connection = mysql.connector.connect(
      user= mysql_cred['user'],
      password= mysql_cred['password'],
      database = mysql_cred['database']
)

cursor = connection.cursor()


def generate_users():
        users = [
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "age": fake.random.randint(22,50),
                "gender": fake.random.choice(['male', 'female', 'others']),
                "email": fake.unique.email()
            }
            for i in range(5)
        ]
        insert_query="""
        INSERT INTO health_metrics_3.users(first_name, last_name, age, gender, email) VALUES(%s, %s, %s, %s, %s)
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




with DAG(
    dag_id= "generate_synthetic_users_health_metrics",
    start_date = datetime(2024, 12, 2),
    schedule_interval='@daily',
    default_args= default_args
) as dag:
    start_mysql = BashOperator(
        task_id= "start_mysql",
        bash_command= "sudo service mysql start"
    )

    import_db = BashOperator(
        task_id= "import_db",
        bash_command= f"sudo mysql -h {mysql_cred['host']} -P {mysql_cred['port']} -u {mysql_cred['username']} -p < health_metrics_3.sql"
    )





