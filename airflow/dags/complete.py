from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from faker import Faker
import mysql.connector
import smtplib
from email.mime.text import MIMEText


fake = Faker()

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

# mysql_cred = {
#     "username": "admin",
#     "port": 3306,
#     "host": "localhost",
#     "password": "1234",  
# }

# connection = mysql.connector.connect(
#       user= mysql_cred['username'],
#       password= mysql_cred['password'],
# )

# cursor = connection.cursor(dictionary=True)
connection, cursor = None

def mysql_login(user= "admin", password= 1234):
    try:
        global connection, cursor
        connection = mysql.connector.connect(
        user= user,
        password= password
        )

        cursor = connection.cursor(dictionary= True)
    except Exception as e:
        print(f"mysql.connect failed: {e}")


def generate_users(users_count):

    global connection, cursor

    users = [
        {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": fake.random.randint(22,50),
            "gender": fake.random.choice(['male', 'female', 'others']),
            "email": fake.email()
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

    print(f"users generated successfully, sample: {users[0]}")


def generate_metrics(users_count):

    global connection, cursor

    data = [
        {
            "user_id": fake.random.randint(1, users_count),
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

    print(f"metrics generated successfully, sample: {data[0]}")


def send_email(to_email, metrics, server= "smtp.gmail.com", port= 587, username= "amustee22@gmail.com", password= "mjtogcqrrttddusp"):
        
        subject = "Your Daily Health Metrics"
        body = f"""
        Hello {metrics['first_name']},

        Here are your aggregated health metrics for the day:
        - Average Heart Rate: {round(metrics['avg_heart_rate'])} bpm, {round(metrics['avg_heart_rate_percent_change'])}% change from yesterday
        - Average Blood Oxygen: {round(metrics['avg_blood_oxygen'])}%, {round(metrics['avg_blood_oxygen_percent_change'])}% change from yesterday
        - Total Steps: {round(metrics['total_steps_count'])}, {round(metrics['total_steps_count_percent_change'])}% change from yesterday
        - Total Calories Burned: {round(metrics['total_calories_burned'])}, {round(metrics['total_calories_burned_percent_change'])}% change from yesterday
        - Average Body Temperature: {round(metrics['avg_body_temperature'])}\u00B0C, {round(metrics['avg_body_temperature_percent_change'])}% change from yesterday
        - Average Stress Level: {round(metrics['avg_stress_level'])}, {metrics['avg_stress_level_change']} from yesterday
        - Activity Level: {metrics['avg_activity_level']}, {metrics['avg_activity_level_change']} from yesterday

        Best regards,
        Abubakar Mustapha Aminu
        """.ljust()

        # Create and send the email
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = username
        msg['To'] = to_email

        with smtplib.SMTP(server, port) as server:
            print(f"sending email to {to_email}")
            server.starttls()
            server.login(username, password)
            response = server.send_message(msg)

            print(f"email successfully sent to {to_email}") if not response else print(f"email to {to_email} failed")


def read_health_metrics_task():

    reports = {}
    try:
        global connection
        global cursor
        cursor.execute("USE health_metrics_4")
        cursor.execute("UPDATE health_metrics_4.users SET email = 'amustee22@gmail.com' WHERE user_id = 1")
        cursor.callproc('health_metrics_4.agg_metrics')

        print("aggregated metrics: \n")
        if cursor.stored_results():
            for result in cursor.stored_results():
                rows = result.fetchall()
                for row in rows:
                    reports[row['email']] = row
                print(rows)
        else: print("stored result empty")

    except Exception as e:
        print(f"read_health_metrics_task error: {e}")
    finally:
        print("reports", reports)
        cursor.close()
        connection.close()

    for email, metrics in reports.items():
        try:
            send_email(email, metrics)
        except Exception as e:
            print(f"send_email to {email} task error {e}")



with DAG(
    dag_id= "complete_project",
    start_date = datetime(2024, 12, 2),
    schedule_interval='@daily',
    default_args= default_args,
    catchup= False
) as dag:
    
    start_mysql = BashOperator(
        task_id = "start_mysql",
        bash_command= "sudo service mysql start"
    )

    mysql_login_task = PythonOperator(
        task_id= "mysql_login",
        python_callable= mysql_login
    )

    import_db = MySqlOperator(
        task_id= "import_db",
        mysql_conn_id='mysql_u_admin',
        sql='health_metrics_3.sql',    
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

    send_emails = PythonOperator(
        task_id = "send_emails",
        python_callable = read_health_metrics_task
    )


    start_mysql >> import_db >> [generate_users_task, generate_metrics_taks] >> send_emails





