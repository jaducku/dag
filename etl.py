from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.exceptions import AirflowSkipException

# 전체 Task 상태 조회 함수
def check_task_states(**context):
    ti = context['ti']
    dag_run = context['dag_run']
    all_tasks = dag_run.get_task_instances()
    
    running_tasks = [task.task_id for task in all_tasks if task.state == State.RUNNING]
    
    # RUNNING 상태의 Task ID 리스트를 XCom으로 저장
    ti.xcom_push(key='running_tasks', value=running_tasks)

# XCom 데이터 삭제 함수
def clear_xcom(task_id, **context):
    ti = context['ti']
    ti.xcom_pull(key='running_tasks', task_ids='check_running_tasks')
    # 해당 Task ID와 관련된 XCom 데이터를 삭제합니다.
    context['ti'].xcom_delete(key='running_tasks')

# 개별 Task 실행 함수
def execute_task(task_id, **context):
    ti = context['ti']
    running_tasks = ti.xcom_pull(key='running_tasks', task_ids='check_running_tasks')
    
    if task_id in running_tasks:
        raise AirflowSkipException(f"Task {task_id} is already running, skipping.")

    # 여기서 실제 작업 수행
    print(f"Executing {task_id}")

with DAG('dag_dynamic_task_skip',
         start_date=days_ago(1),
         schedule_interval="@daily") as dag:

    # 전체 Task 상태를 체크하는 Task
    check_running_tasks = PythonOperator(
        task_id='check_running_tasks',
        python_callable=check_task_states,
        provide_context=True,
        dag=dag
    )

    # 동적으로 생성되는 Task
    task_ids = ['task_1', 'task_2', 'task_3']
    tasks = []

    for task_id in task_ids:
        task = PythonOperator(
            task_id=task_id,
            python_callable=execute_task,
            op_args=[task_id],
            provide_context=True,
            on_success_callback=clear_xcom,  # Task 성공 시 XCom 삭제
            on_failure_callback=clear_xcom,  # Task 실패 시 XCom 삭제
            dag=dag
        )
        tasks.append(task)
        check_running_tasks >> task