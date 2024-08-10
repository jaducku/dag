from airflow.models import BaseOperator, TaskInstance
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

class SkipIfRunningOperator(BaseOperator):

    @provide_session
    def execute(self, context, session=None):
        task_instance = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == context['execution_date'],
            TaskInstance.state == State.RUNNING
        ).first()

        if task_instance:
            raise AirflowSkipException(f"Task {self.task_id} is already running, skipping.")

        # 실제 Task 수행할 작업 (Task가 실행 중이지 않은 경우에만 수행)
        # task_logic()

with DAG('dynamic_task_skip_example',
         start_date=days_ago(1),
         schedule_interval='*/1 * * * *') as dag:

    tasks = []
    task_ids = ['task_1', 'task_2', 'task_3']
    
    for task_id in task_ids:
        task = SkipIfRunningOperator(
            task_id=task_id,
            dag=dag
        )
        tasks.append(task)