from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from minio import Minio
import os
import subprocess

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# DAG 정의
dag = DAG(
    'defect-data',
    default_args=default_args,
    description='A DAG to process files from MinIO',
    schedule_interval=None,
)

# Task 1: MinIO에서 파일 다운로드
def download_from_minio(object_key, download_dir):
    minio_client = Minio(
        Variable.get("MINIO_ENDPOINT"),
        access_key=Variable.get("MINIO_ACCESS_KEY"),
        secret_key=Variable.get("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket_name, file_name = object_key.split("/", 1)
    local_file_path = os.path.join(download_dir, file_name)

    minio_client.fget_object(bucket_name, file_name, local_file_path)
    print(f"Downloaded {object_key} to {local_file_path}")

# Task 2: JAR 파일 실행
def run_jar(local_file_path):
    jar_path = Variable.get("JAR_PATH")
    result = subprocess.run(['java', '-jar', jar_path, local_file_path], capture_output=True, text=True)
    print(f"JAR output: {result.stdout}")
    if result.returncode != 0:
        raise Exception(f"JAR file failed with return code {result.returncode}")

download_task = PythonOperator(
    task_id='download_from_minio',
    python_callable=download_from_minio,
    op_kwargs={'object_key': '{{ dag_run.conf["object_key"] }}', 'download_dir': '/tmp/downloads'},
    dag=dag,
)

run_jar_task = PythonOperator(
    task_id='run_jar',
    python_callable=run_jar,
    op_kwargs={'local_file_path': '/tmp/downloads/{{ dag_run.conf["object_key"].split("/", 1)[1] }}'},
    dag=dag,
)

download_task