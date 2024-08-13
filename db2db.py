from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import time
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def my_task(task_id):
    large_list = [0.0] * 3_750_000
    print(f"Task {task_id} executed")
    time.sleep(random.uniform(5, 10))

with DAG(
    'my_dynamic_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    catchup=False,
    max_active_runs=1,
) as dag:
    
    start = DummyOperator(
        task_id='start',
    )
    
    dynamic_tasks = []
    for i in range(20):  # 예시로 5개의 태스크를 동적으로 생성
        task = PythonOperator(
            task_id=f'main_task_{i}',
            python_callable=my_task,
            op_args=[i],
        )
        dynamic_tasks.append(task)
        start >> task
    
    complete = DummyOperator(
        task_id='complete',
        trigger_rule=TriggerRule.ALL_DONE,  # 모든 동적 태스크가 완료된 후에 실행
    )
    
    for task in dynamic_tasks:
        task >> complete