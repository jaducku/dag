from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.utils.state import State
from datetime import datetime, timedelta
import time
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'db2db',
    default_args=default_args,
    description='A DAG to dynamically skip running tasks using BranchPythonOperator',
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(1),
    catchup=False,
)

def my_task(task_number, **kwargs):
    time.sleep(random.randint(40, 90))
    print(f"Executing task number {task_number}")

# 시작 태스크
start = DummyOperator(
    task_id='start',
    dag=dag,
)

for i in range(10):
    task_id = f'task_{i}'

    etl_main = PythonOperator(
        task_id=task_id,
        python_callable=my_task,
        op_kwargs={'task_number': i},
        dag=dag,
    )

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='all_done',
)

start >> etl_main >> end