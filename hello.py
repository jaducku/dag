from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time

# 함수 정의
def say_hello():
    print("hello")
    time.sleep(2)
    print("Completed")

# 기본 인수 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=None,  # 스케줄링 없음
    max_active_runs=50
) as dag:

    # 작업 정의
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

# DAG 등록
hello_task