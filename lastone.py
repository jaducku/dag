import time
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def use_memory_and_time(task_id):
    # 30MB 메모리를 사용하기 위해 약 3,750,000개의 float 요소를 가진 리스트 생성
    large_list = [0.0] * 3_750_000
    
    # 40초에서 90초 사이의 랜덤한 시간 동안 처리 시간 추가
    sleep_time = random.uniform(40, 90)
    print(f"Task {task_id} will sleep for {sleep_time:.2f} seconds.")
    time.sleep(sleep_time)
    print(f"Task {task_id} completed.")

with DAG(
    'last-dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    catchup=False,
    max_active_runs=1,  # 동시에 실행될 수 있는 DAG 인스턴스를 1개로 제한
    concurrency=5,  # 동시에 실행될 수 있는 태스크의 수를 5로 제한
) as dag:
    
    latest_only = LatestOnlyOperator(
        task_id='latest_only'
    )
    
    start = DummyOperator(
        task_id='start',
    )
    
    dynamic_tasks = []
    for i in range(5):  # 예시로 5개의 태스크를 동적으로 생성
        task = PythonOperator(
            task_id=f'main_task_{i}',
            python_callable=use_memory_and_time,
            op_args=[i],
        )
        dynamic_tasks.append(task)
        latest_only >> start >> task
    
    complete = DummyOperator(
        task_id='complete',
        trigger_rule='all_done',  # 모든 동적 태스크가 완료된 후에 실행
    )
    
    for task in dynamic_tasks:
        task >> complete