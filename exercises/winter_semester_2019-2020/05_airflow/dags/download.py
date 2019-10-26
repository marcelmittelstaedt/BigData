from datetime import datetime
from airflow import DAG
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator

args = {
    'owner': 'airflow'
}

dag = DAG('download_file', default_args=args, description='Simple File Operations  DAG',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)


download_file = HttpDownloadOperator(
    task_id='download_file',
    download_uri='https://datasets.imdbws.com/title.ratings.tsv.gz',
    save_to='/home/airflow/title.ratings.tsv.gz',
    dag=dag,
)

unzip_file = UnzipFileOperator(
    task_id='unzip_file',
    zip_file='home/airflow/title.ratings.tsv.gz',
    extract_to='/home/airflow/title.ratings.tsv',
    dag=dag,
)

download_file >> unzip_file
