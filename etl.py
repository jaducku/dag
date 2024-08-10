from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException
import time
import random
# 전체 Task 상태를 확인하고 딕셔너리에 저장하는 함수
def check_all_task_states(**kwargs):
    dag_id = kwargs['dag'].dag_id
    current_execution_date = kwargs['execution_date']
    
    # 현재 DAG에서 실행 중인 Task들의 상태를 딕셔너리에 저장
    task_states = {}
    running_dag_runs = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    
    for dag_run in running_dag_runs:
        if dag_run.execution_date != current_execution_date:
            task_instances = dag_run.get_task_instances()
            for task_instance in task_instances:
                task_states[task_instance.task_id] = task_instance.state
    
    # 이 딕셔너리를 반환하여 다음 Task로 전달
    return task_states

# 개별 Task 실행 함수
def execute_task_with_check(task_id, task_states, **kwargs):
    # 전달된 딕셔너리에서 해당 Task의 상태를 확인하고, 이미 실행 중이면 Skip
    if task_states.get(task_id) == State.RUNNING:
        raise AirflowSkipException(f"Task {task_id} is already running, skipping.")
    
    # 여기서 실제 작업 수행
    print(f"Executing {task_id}")
    time.sleep(random.uniform(40, 90))

with DAG('dag_task_state_check_no_xcom',
         start_date=days_ago(1),
         schedule_interval='*/1 * * * *',) as dag:

    # 전체 Task 상태를 확인하는 Task
    check_task_states = PythonOperator(
        task_id='check_task_states',
        python_callable=check_all_task_states,
        provide_context=True,
        dag=dag
    )

    # 동적으로 생성되는 Task
    task_ids = ['task_1', 'task_2', 'task_3']
    tasks = []

    for task_id in task_ids:
        task = PythonOperator(
            task_id=task_id,
            python_callable=execute_task_with_check,
            op_args=[task_id],  # Task ID 전달
            op_kwargs={"task_states": check_task_states.output},  # Task 상태 딕셔너리 전달
            provide_context=True,
            dag=dag
        )
        tasks.append(task)
        check_task_states >> task