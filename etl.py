from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.models import TaskInstance

# Task 상태를 직접 확인하는 함수
def check_task_state(task_id, **kwargs):
    dag_run = kwargs['dag_run']
    task_instances = dag_run.get_task_instances()

    # 각 Task의 상태를 확인
    for task_instance in task_instances:
        if task_instance.task_id == task_id and task_instance.state == State.RUNNING:
            # Task가 RUNNING 상태면 Skip 경로로 이동
            return f"skip_{task_id}"
    
    # 그렇지 않으면 실행 경로로 이동
    return task_id

# 개별 Task 실행 함수
def execute_task(task_id, **kwargs):
    print(f"Executing {task_id}")

# Skip될 때 사용하는 Dummy 함수
def skip_task(**kwargs):
    print("Task skipped.")

with DAG('dag_branch_operator_no_xcom',
         start_date=days_ago(1),
         schedule_interval="@daily") as dag:

    # 동적으로 생성되는 Task
    task_ids = ['task_1', 'task_2', 'task_3']
    tasks = []

    for task_id in task_ids:
        branch = BranchPythonOperator(
            task_id=f'branch_{task_id}',
            python_callable=check_task_state,
            op_args=[task_id],
            provide_context=True,
            dag=dag
        )

        task = PythonOperator(
            task_id=task_id,
            python_callable=execute_task,
            op_args=[task_id],
            provide_context=True,
            dag=dag
        )

        skip = PythonOperator(
            task_id=f'skip_{task_id}',
            python_callable=skip_task,
            dag=dag
        )

        tasks.append(task)
        branch >> [task, skip]