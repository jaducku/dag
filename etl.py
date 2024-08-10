
from airflow import DAG
from airflow import settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from sqlalchemy.orm import joinedload
import time
import random

# DAG Define
dag_name = 'db2db_etl_lot_history'
running_tasks = {}

def get_running_tasks_at_dat():
    session = settings.Session()

    running_tasks = session.query(TaskInstance).join(DagRun, DagRun.execution_date == TaskInstance.execution_date).filter(
        TaskInstance.dag_id == dag_name,
        TaskInstance.state in [State.RUNNING,State.QUEUED,State.SCHEDULED],
        DagRun.dag_id == TaskInstance.dag_id
    ).options(joinedload(TaskInstance.dag_run)).all()

def etl_main(task_id):
    if any(task.task_id == task_id for task in running_tasks):
        print(f'hello {task_id}')
        time.sleep(random.uniform(1, 90))
    else:
        print(f'task is running now : {task_id}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_name,
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='*/1 * * * *',
) as dag:

    start = DummyOperator(
        task_id='start',
    )
    
    for i in range(5):  # 예시로 5개의 태스크를 동적으로 생성
        task_id = f'{dag_name}_task_{i}'
        task = PythonOperator(
            task_id=task_id,
            python_callable=etl_main,
            op_args=[task_id],
        )
        
        start >> task