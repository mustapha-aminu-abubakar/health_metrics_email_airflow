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


# creating a Faker object, to be used to generate synthetic data
fake = Faker()


# dag's default arguments
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}



###################################################
###################################################
# replace with a name and email of your choice
sample_name = "Musty"
sample_email = "amustee22@gmail.com" # a sample health metrics summary be sent to this address
###################################################
###################################################



# specifying number of synthetic users to be generated in the database
users_count = 5


def mysql_login(user= "admin", password= "1234"):
    """
    Initiates a connection to the local mysql server

    Args:
        user (str): mysql user. Defaults to "admin".
        password (str): passwrd for specified mysql user. Defaults to "1234".

    Returns:
        connection object
    """
    try:
        connection = mysql.connector.connect(
        user= user,
        password= password
        )
        return connection

    except Exception as e:
        print(f"mysql.connector.connect() failed: {e}")       

    finally:
        print(f"mysql logged in ?: {connection.is_connected()}")


def generate_users():
    """
    Generates synthetic users and inserts them into health_metrics.users table
    """    
    
    global users_count
    connection = mysql_login() # calling mysql_login function and connecting to local mysql server
    cursor = connection.cursor(dictionary=True) # cursor object to execute mysql statements

    # generating 'users_count' number of users ; first & last names, age, gender and email.
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
    
    # printing number of users generated
    print(f"# of users generated: {len(users)}")
    
    # creating a DML insert query as a string with placeholder values (%s)
    insert_query="""
    INSERT INTO health_metrics.users(first_name, last_name, age, gender, email) VALUES(%s, %s, %s, %s, %s)
    """

    # formating the 'users' list of dicts to a list of tuples
    users_tuples=[(
        user['first_name'],
        user['last_name'],
        user['age'],
        user['gender'],
        user['email']
    ) for user in users]

    # executing 'insert_query' using 'users_tuples' as values
    try:
        cursor.executemany(insert_query, users_tuples)
        connection.commit()     # committing changes to the database
        print(f"users inserted successfully, sample: {users[0]}")

    except Exception as e:
        print(f"generate_users failed: {e}")

    finally:    # closing cursor and connection to save computing resources
        cursor.close()
        connection.close()


def generate_metrics():
    """
    Generates synthetic health metrics and inserts them into health_metrics.metrics table
    """    

    global users_count
    connection = mysql_login()      # calling mysql_login function and connecting to local mysql server
    cursor = connection.cursor(dictionary=True)         # cursor object to execute mysql statements

    # generating 2000 records each for 'users_count' number of users of the following metrics
    # heart rate, blood_oxygen, steps_count, calories_burned, body_temperature
    data = [
        {
            "user_id": fake.random.randint(1, users_count),     # random user_id
            "date_time": fake.unique.date_time_between_dates(datetime.today() - timedelta(days=3), datetime.today()),   #within the last 3 days
            "heart_rate": fake.random.randint(50, 100),     # beats per minute
            "blood_oxygen": round(fake.random.uniform(95, 100), 1),     # as percentage
            "steps_count": fake.random.randint(0, 20000),   # steps
            "calories_burned": round(fake.random.uniform(50, 500), 1),  # kcal
            "body_temperature": round(fake.random.uniform(36.0, 37.5), 1),  # Celsius
        }
        for _ in range(users_count * 2000)
    ]

    # creating a DML insert query as a string with placeholder values (%s)
    insert_query = """
        INSERT INTO health_metrics.metrics VALUES (%s, %s, %s, %s, %s, %s, %s)
    """            

    # formatting 'data' list of dicts to a list of tuples
    data_tuples = [(
                record['user_id'],
                record['date_time'], 
                record['heart_rate'], 
                record['blood_oxygen'], 
                record['steps_count'], 
                record['calories_burned'], 
                record['body_temperature']) for record in data]

    # printing number of rows generated
    print(f"{len(data_tuples)} rows generated")
    
    # executing 'insert_query' using 'data_tuples' as values
    try:
        cursor.executemany(insert_query, data_tuples)
        connection.commit()     # committing changes to the database
        print(f"metrics inserted successfully, sample: {data[0]}")

    except Exception as e:
        print(f"generate_metrics failed: {e}")

    finally:    # closing cursor and connection to save computing resources
        cursor.close()
        connection.close()


def send_email(to_email, metrics, server= "smtp.gmail.com", port= 587, username= "amustee22@gmail.com", password= "mjtogcqrrttddusp"):
    """
    Sends a summary of health_metrics to specified email for the associated user.

    Args:
        to_email (str): destination email address
        metrics (dict): aggregated health metrics for associated user
        server (str): mail server of sender email. Defaults to "smtp.gmail.com".
        port (int): mail server port of sender email. Defaults to 587.
        username (str): sender email address. Defaults to "amustee22@gmail.com".
        password (str): (App) password. Defaults to "mjtogcqrrttddusp".
    """
             
    subject = "Your Daily Health Metrics"
    
    # template of html content of email(s) to be sent 
    body = f"""
        <!DOCTYPE html>
        <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
                *{{
                    box-sizing: border-box;
                }}
                

                body{{
                    max-width: 400px;
                }}

                table{{
                    margin: 20px auto;
                    width: 100%;
                    border-collapse: collapse;
                }}

                th {{
                    font-weight: bold;
                    padding: 20px auto;
                    text-align: left;
                }}

                td {{
                    padding: 20px 10px;
                }}

                tbody tr{{
                    border-bottom: 1px solid #ccc;
                }}

                thead tr{{
                    border-bottom: 1px solid #222;
                }}

                
            </style>
        </head>
        <body>
        <p> Hello <b>{metrics['first_name']}</b>, here is a summary of your daily health metrics for <b>{metrics['date']}</b> from <a href="https://github.com/mustapha-aminu-abubakar/health_metrics_email_airflow/tree/main"> Health metrics dag</a></p>
            <table>
                <colgroup>
                <col style="width: 50%">
                <col style="width: 30%">
                <col style="width: 20%">
                </colgroup>
                <thead>
                    <tr>
                        <th> Metric </th>
                        <th> Value  </th>
                        <th> Change<small>*</small></th>
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
                        <td> Body temperature (\u00B0C)</td>
                        <td> {round(metrics['avg_body_temperature'], 1)}</td>
                        <td> {round(metrics['avg_body_temperature_percent_change'])}% </td>
                    </tr>
                </tbody>
            </table>
            <br>
            <p>Best regards, Mustapha Aminu <br>
            <a href="https://github.com/mustapha-aminu-abubakar">Github</a><br>
            <a href="https://linkedin.com/in/mustapha-aminu-a2a685229">LinkedIn</a><br>
            </p>

            <br>
            <p style="color: grey"><small>*<i>Change from {(metrics['date'] - timedelta(days=1)).strftime('%Y-%m-%d')}</i></small></p>
            <br>
            <p style="color: grey"><small><i>Disclaimer: The data presented in this email is entirely fictitious and is intended solely for educational and demonstration purposes.</i></small></p>        
            <br>
            <p style="color: grey"><small><i>If you received this email in error, please disregard it and delete it immediately. No action is required on your part.</i></small></p>
            </body>
        </html>
    """
    
    # adding metatdata to the email template including sender and recipient email addresses
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = username
    msg['To'] = to_email
    
    # parsing and attaching html content to message
    body_html = MIMEText(body, "html")
    msg.attach(body_html)

    # sending email from 'server' through 'port' using login credentials; 'username' & 'password' 
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
    """
    Aggregates health metrics and passes them to send_email function
    """    
    
    global sample_name, sample_email
    reports = {}
    try:
        connection = mysql_login()      # calling mysql_login function and connecting to local mysql server
        cursor = connection.cursor(dictionary=True)     # cursor object to execute mysql statements

        # replaces the first user's name and email address with provided sample name and email address
        cursor.execute("USE health_metrics")
        cursor.execute(f"UPDATE health_metrics.users SET email = '{sample_email}', first_name ='{sample_name.split()[0]}'  WHERE user_id = 1")
        
        # calls stored procedure that aggregates health metrics
        cursor.callproc('health_metrics.agg_metrics')

        # appends aggregared metrics as dictionaries to 'reports' dict, with email addresses as keys
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
    finally:    # closing cursor and connection to save computing resources
        print("reports", reports)
        cursor.close()
        connection.close()

    # calls send_email() for every user, containing their respective daily aggregated health metrics
    for email, metrics in reports.items():
        try:
            send_email(email, metrics)
        except Exception as e:
            print(f"send_email to {email} task error {e}")


# Initiating DAG object
with DAG(
    dag_id= "health_metrics_dag",
    start_date = datetime(2024, 12, 2),
    schedule_interval='@daily',
    default_args= default_args,
    catchup= False
) as dag:
    #defining tasks
    
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

    # defining dependencies
    start_mysql >> import_db >> [generate_users_task, generate_metrics_taks] >> send_emails





