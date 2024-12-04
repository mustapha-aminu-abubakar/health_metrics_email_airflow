from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from faker import Faker
import mysql.connector
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


fake = Faker()

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

sample_name = "Musty"
sample_email = "amustee22@gmail.com"


users_count = 5

def mysql_login(user= "admin", password= "1234"):
    try:
        connection = mysql.connector.connect(
        user= user,
        password= password
        )
        return connection

    except Exception as e:
        print(f"mysql.connect failed: {e}")       

    finally:
        print(f"mysql_login: {connection.is_connected()}")


def generate_users():

    global users_count
    connection = mysql_login()
    cursor = connection.cursor(dictionary=True)

    users = [
        {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": fake.random.randint(22,50),
            "gender": fake.random.choice(['male', 'female', 'others']),
            "email": fake.email()
        }
        for _ in range(users_count)
    ]

    print(f"# of users generated: {len(users)}")
    
    insert_query="""
    INSERT INTO health_metrics.users(first_name, last_name, age, gender, email) VALUES(%s, %s, %s, %s, %s)
    """

    users_tuples=[(
        user['first_name'],
        user['last_name'],
        user['age'],
        user['gender'],
        user['email']
    ) for user in users]

    try:
        cursor.executemany(insert_query, users_tuples)
        connection.commit()
        print(f"users inserted successfully, sample: {users[0]}")

    except Exception as e:
        print(f"generate_users failed: {e}")

    finally:
        cursor.close()
        connection.close()


def generate_metrics():

    global users_count
    connection = mysql_login()
    cursor = connection.cursor(dictionary=True)

    data = [
        {
            "user_id": fake.random.randint(1, users_count),
            "date_time": fake.unique.date_time_between_dates(datetime.today() - timedelta(days=3), datetime.today()),
            "heart_rate": fake.random.randint(50, 100),  # beats per minute
            "blood_oxygen": round(fake.random.uniform(95, 100), 1),  # percentage
            "steps_count": fake.random.randint(0, 20000),  # steps
            "calories_burned": round(fake.random.uniform(100, 1000), 2),  # kcal
            "body_temperature": round(fake.random.uniform(36.0, 37.5), 1),  # Celsius
        }
        for _ in range(users_count * 2000)
    ]

    insert_query = """
        INSERT INTO health_metrics.metrics VALUES (%s, %s, %s, %s, %s, %s, %s)
    """            

    data_tuples = [(
                record['user_id'], 
                record['date_time'], 
                record['heart_rate'], 
                record['blood_oxygen'], 
                record['steps_count'], 
                record['calories_burned'], 
                record['body_temperature']) for record in data]

    print(f"{len(data_tuples)} rows generated")
    
    try:
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        print(f"metrics inserted successfully, sample: {data[0]}")

    except Exception as e:
        print(f"generate_metrics failed: {e}")

    finally:
        cursor.close()
        connection.close()


def send_email(to_email, metrics, server= "smtp.gmail.com", port= 587, username= "amustee22@gmail.com", password= "mjtogcqrrttddusp"):
        
        subject = "Your Daily Health Metrics"
        
        body = f"""
            <!DOCTYPE html>
            <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                    *{{
                        box-sizing: border-box
                    }}
                    
                    .header{{
                        font-size: 1.2em
                    }}

                    table{{
                        margin: 20px auto;
                        width: 100%;
                        max-width: 400px
                    }}

                    th {{
                        font-weight: bold;
                        padding: 20px auto;
                        text-align: center;
                    }}

                    td {{
                        padding: 20px 10px;
                    }}

                   
                </style>
            </head>
            <body>
            <p class="header"> Hello <b>{metrics['first_name']}</b>, here is a summary of your daily health metrics for <b>{metrics['date']}</b> </p>
                <table>
                    <colgroup>
                    <col style="width: 40%">
                    <col style="width: 30%">
                    <col style="width: 30%">
                    </colgroup>
                    <thead>
                        <tr>
                            <th> Metric </th>
                            <th> Value  </th>
                            <th> Change (%) </th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td> Heart rate (bpm) </td>
                            <td> {round(metrics['avg_heart_rate'])} </td>
                            <td> {round(metrics['avg_heart_rate_percent_change'])}% </td>
                        </tr>
                        <tr>
                            <td> Blood oxygen (%) </td>
                            <td> {round(metrics['avg_blood_oxygen'])} </td>
                            <td> {round(metrics['avg_blood_oxygen_percent_change'])}% </td>
                        </tr><tr>
                            <td> Steps </td>
                            <td> {metrics['total_steps_count']} </td>
                            <td> {round(metrics['total_steps_count_percent_change'])}% </td>
                        </tr><tr>
                            <td> Calories burned (kcal) </td>
                            <td> {round(metrics['total_calories_burned'] / 1000)} </td>
                            <td> {round(metrics['total_calories_burned_percent_change'])}% </td>
                        </tr><tr>
                            <td> Body temperature </td>
                            <td> {round(metrics['avg_body_temperature'], 1)}\u00B0C </td>
                            <td> {round(metrics['avg_body_temperature_percent_change'])}% </td>
                        </tr>
                    </tbody>
                </table>
                <p>Best regards, Mustapha Aminu</p>
                
            </body>
            </html>
        """
        msg = MIMEMultipart("alternative")
        msg['Subject'] = subject
        msg['From'] = username
        msg['To'] = to_email
        
        body_html = MIMEText(body, "html")
        msg.attach(body_html)


        with smtplib.SMTP(server, port) as server:
            try:
                print(f"sending email to {to_email}")
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
                print(f"email successfully sent to {to_email}") 
            except Exception as e:
                print(f"email to {to_email} failed: {e}")


def agg_metrics_and_send_mail():
    
    global sample_name, sample_email
    reports = {}
    try:
        connection = mysql_login()
        cursor = connection.cursor(dictionary=True)

        cursor.execute("USE health_metrics")
        cursor.execute(f"UPDATE health_metrics.users SET email = '{sample_email}', first_name ='{sample_name.split()[0]}'  WHERE user_id = 1")
        cursor.callproc('health_metrics.agg_metrics')

        print("aggregated metrics: \n")
        if cursor.stored_results():
            for result in cursor.stored_results():
                rows = result.fetchall()
                for row in rows:
                    reports[row['email']] = row
                print(rows)
        else: print("stored result empty")

    except Exception as e:
        print(f"agg_metrics_and_send_mail error: {e}")
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
    dag_id= "complete_project_2",
    start_date = datetime(2024, 12, 2),
    schedule_interval='@daily',
    default_args= default_args,
    catchup= False
) as dag:
    
    start_mysql = BashOperator(
        task_id = "start_mysql",
        bash_command= "sudo service mysql start"
    )


    import_db = MySqlOperator(
        task_id= "create_database_and_tables",
        mysql_conn_id='mysql_u_admin',
        sql='create_database_and_tables.sql',    
    )


    generate_users_task = PythonOperator(
        task_id= "generate_users",
        python_callable= generate_users
    )

    generate_metrics_taks = PythonOperator(
        task_id = "generate_metrics",
        python_callable= generate_metrics
    )

    send_emails = PythonOperator(
        task_id = "send_emails",
        python_callable = agg_metrics_and_send_mail
    )


    start_mysql >> import_db >> [generate_users_task, generate_metrics_taks] >> send_emails





