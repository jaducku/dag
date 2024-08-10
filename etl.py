from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException
import time
import random
from datetime import datetime


# 전체 Task 상태를 확인하고 딕셔너리에 저장하는 함수
def check_task_statuses(dag_id, execution_date=None):
    # execution_date가 없으면 가장 최근 실행을 조회
    execution_date = execution_date or datetime.utcnow()

    # DAG 실행 가져오기
    dag_runs = DagRun.find(dag_id=dag_id, execution_date=execution_date)
    
    if not dag_runs:
        print(f"No DAG run found for DAG ID {dag_id} at {execution_date}")
        return

    dag_run = dag_runs[0]  # 가장 최근의 DAG Run

    # 해당 DAG Run에 속한 모든 Task 인스턴스 가져오기
    task_instances = TaskInstance.find(dag_id=dag_id, execution_date=dag_run.execution_date)
    
    for task_instance in task_instances:
        print(f"Task {task_instance.task_id} is in state {task_instance.state}")
    time.sleep(random.uniform(40, 90))

with DAG('task_check',
         start_date=days_ago(1),
         schedule_interval='*/1 * * * *',) as dag:

    # 전체 Task 상태를 확인하는 Task
    check_task_states = PythonOperator(
        task_id='check_task_states',
        python_callable=check_task_statuses,
        dag=dag
    )

    check_task_states 