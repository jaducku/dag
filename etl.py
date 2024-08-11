from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from airflow.utils.dates import days_ago
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException
import time
import random
from datetime import datetime

dag_name = 'task_check'
# 전체 Task 상태를 확인하고 딕셔너리에 저장하는 함수
@provide_session
def check_task_statuses(dag_id, execution_date=None, session=None):
    # execution_date가 없으면 가장 최근 실행을 조회
    if execution_date is None:
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
    else:
        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date).first()

    if not dag_run:
        print(f"No DAG run found for DAG ID {dag_id} at {execution_date}")
        time.sleep(70)
        return

    # 해당 DAG Run에 속한 모든 Task 인스턴스 가져오기
    task_instances = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.execution_date == dag_run.execution_date).all()

    for task_instance in task_instances:
        print(f"Task {task_instance.task_id} is in state {task_instance.state}")
   
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    'retries': 1,
    'retry_delay': 1,
}

with DAG(
    'task_check',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    catchup=False,
) as dag:
    
    task_id = 'check_task_states'
    # 전체 Task 상태를 확인하는 Task
    check_task_states = PythonOperator(
        task_id=task_id,
        python_callable=check_task_statuses,
        op_args=[dag_name],
        dag=dag
    )

    check_task_states 