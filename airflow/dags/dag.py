from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id = 'test_dag',
    default_args= default_args,
    schedule_interval= '0 0 * * *',
    start_date= datetime(2024, 11, 28)
)

def test_dag():

    @task
    def test_task():
        print(datetime.today())
    
    test_task = test_task()
test_dag = test_dag()