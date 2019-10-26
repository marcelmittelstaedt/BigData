from datetime import datetime
from airflow import DAG
from airflow.operators.filesystem_operations import CopyFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator

args = {
    'owner': 'airflow'
}

dag = DAG('file_ops', default_args=args, description='Simple File Operations  DAG',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

copy_file = CopyFileOperator(
    task_id='copy_file',
    source='/home/airflow/test.txt',
    dest='/home/airflow/file_{{ ds }}.txt',
    overwrite=True,
    dag=dag,
)

create_dir = CreateDirectoryOperator(
    task_id='create_directory',
    path='/home/airflow',
    directory='testdir',
    dag=dag,
)

clear_dir = ClearDirectoryOperator(
    task_id='clear_directory',
   # paramm='sdf',
    directory='/home/airflow/clear_directory',
    pattern='*.txt',
    dag=dag,
)

clear_dir >> create_dir >> copy_file
