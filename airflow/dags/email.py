import smtplib
from email.mime.text import MIMEText
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'health_metrics'
}

# Email configuration
SMTP_CONFIG = {
    'server': 'smtp.example.com',
    'port': 587,
    'username': 'your_email@example.com',
    'password': 'your_email_password',
}

def aggregate_metrics_and_send_emails():
    # Connect to the database
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)  # Use dictionary=True for column names as keys
    
    try:
        # Call the stored procedure to get aggregated metrics
        cursor.callproc('agg_metrics')  # Replace with your stored procedure name
        
        # Fetch the results
        metrics_result = None
        for result in cursor.stored_results():
            metrics_result = result.fetchall()
        
        # Fetch user emails
        cursor.execute("SELECT user_id, email FROM users")
        users = cursor.fetchall()
        
        # Map emails to metrics
        email_data = {}
        for user in users:
            for metric in metrics_result:
                if user['user_id'] == metric['user_id']:
                    email_data[user['email']] = metric
                    break
        
        # Send emails
        for email, metrics in email_data.items():
            send_email(email, metrics)

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()

def send_email(to_email, metrics):
    sender_email = SMTP_CONFIG['username']
    smtp_server = SMTP_CONFIG['server']
    smtp_port = SMTP_CONFIG['port']
    sender_password = SMTP_CONFIG['password']

    # Create the email content
    subject = "Your Daily Health Metrics"
    body = f"""
    Hello,

    Here are your aggregated health metrics for the day:
    - Average Heart Rate: {metrics['avg_heart_rate']}
    - Average Blood Oxygen: {metrics['avg_blood_oxygen']}
    - Total Steps Count: {metrics['total_steps_count']}
    - Total Calories Burned: {metrics['total_calories_burned']}
    - Average Stress Level: {metrics['avg_stress_level']}
    - Average Body Temperature: {metrics['avg_body_temperature']}
    - Activity Level: {metrics['avg_activity_level']}

    Best regards,
    Health Metrics Team
    """

    # Create and send the email
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = to_email

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG('daily_health_metrics_email',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    email_task = PythonOperator(
        task_id='aggregate_and_email',
        python_callable=aggregate_metrics_and_send_emails,
    )
