from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.utils.state import State
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'branch_operator_skip_task_dag',
    default_args=default_args,
    description='A DAG to dynamically skip running tasks using BranchPythonOperator',
    schedule_interval=timedelta(minutes=10),
    start_date=days_ago(1),
    catchup=False,
)

def choose_branch(task_id, **kwargs):
    ti = TaskInstance(kwargs['dag'].get_task(task_id), kwargs['execution_date'])
    ti.refresh_from_db()

    if ti.state in [State.RUNNING, State.SUCCESS]:
        return f'skip_{task_id}'
    else:
        return task_id

def my_task(task_number, **kwargs):
    print(f"Executing task number {task_number}")

# 시작 태스크
start = DummyOperator(
    task_id='start',
    dag=dag,
)

for i in range(10):
    task_id = f'task_{i}'

    branch_task = BranchPythonOperator(
        task_id=f'branch_{task_id}',
        python_callable=choose_branch,
        op_args=[task_id],
        provide_context=True,
        dag=dag,
    )

    task = PythonOperator(
        task_id=task_id,
        python_callable=my_task,
        op_kwargs={'task_number': i},
        dag=dag,
    )

    skip_task = DummyOperator(
        task_id=f'skip_{task_id}',
        dag=dag,
    )

    start >> branch_task >> [task, skip_task]