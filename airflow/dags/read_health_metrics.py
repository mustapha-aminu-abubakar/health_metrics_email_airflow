from airflow.decorators import dag, task
from datetime import datetime, timedelta
import mysql.connector
import smtplib
from email.mime.text import MIMEText

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
                    reports[row['email']] = row

        except Exception as e:
            print(f"read_health_metrics_task error: {e}")
        finally:
            print(reports)
            cursor.close()
            connection.close()

        try:
            for email, metrics in reports.items():
                # send_email(email, metrics)
                pass
        except Exception as e:
            print(f"send_email task error {e}")

    @task
    def send_email(to_email, metrics, server, port, username, password):
        subject = "Your Daily Health Metrics"
        body = f"""
        Hello,

        Here are your aggregated health metrics for the day:
        - Average Heart Rate: {metrics['avg_heart_rate']}, {metrics['avg_heart_rate_percent_change']} change from yesterday
        - Average Blood Oxygen: {metrics['avg_blood_oxygen']}, {metrics['avg_blood_oxygen_percent_change']} change from yesterday
        - Total Steps Count: {metrics['total_steps_count']}, {metrics['total_steps_count_percent_change']} change from yesterday
        - Total Calories Burned: {metrics['total_calories_burned']}, {metrics['total_calories_burned_percent_change']} change from yesterday
        - Average Stress Level: {metrics['avg_stress_level']}, {metrics['avg_stress_level_percent_change']} change from yesterday
        - Average Body Temperature: {metrics['avg_body_temperature']}, {metrics['avg_body_temperature_percent_change']} change from yesterday
        - Activity Level: {metrics['avg_activity_level']}, {metrics['avg_blood_oxygen_percent_change']} compared to yesterday

        Best regards,
        Abubakar Mustapha Aminu
        """

        # Create and send the email
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = username
        msg['To'] = to_email

    with smtplib.SMTP(server, port) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)





    # read_health_metrics_task = read_health_metrics_task()
# read_health_metrics = read_health_metrics()

        