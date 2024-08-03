from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'maintenance_cleanup',
    default_args=default_args,
    description='A DAG to clean up old metadata and logs',
    schedule_interval='@weekly',
)

cleanup_db = BashOperator(
    task_id='cleanup_db',
    bash_command='airflow db cleanup --clean-before $(date -d "-30 days" +%Y-%m-%d)',
    dag=dag,
)