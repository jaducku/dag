from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'triggered_jar_execution',
    default_args=default_args,
    description='A DAG that runs a JAR file triggered externally',
    schedule_interval=None,  # 스케줄링 없음
    catchup=False,
)

# BashOperator를 사용하여 JAR 파일 실행
run_jar = BashOperator(
    task_id='run_jar',
    bash_command='java -jar /path/to/your/file.jar',
    dag=dag,
)