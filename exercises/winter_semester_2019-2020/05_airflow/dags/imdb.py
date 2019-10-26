# -*- coding: utf-8 -*-

"""
Title: Simple Example Dag 
Author: Marcel Mittelstaedt
Description: 
Just for educational purposes, not to be used in any productive mannor.
Downloads IMDb data, puts them into HDFS and creates HiveTable.
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator

args = {
    'owner': 'airflow'
}

dag = DAG('IMDb', default_args=args, description='IMDb Import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='imdb',
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/imdb',
    pattern='*',
    dag=dag,
)

download_title_ratings = HttpDownloadOperator(
    task_id='download_title_ratings',
    download_uri='https://datasets.imdbws.com/title.ratings.tsv.gz',
    save_to='/home/airflow/imdb/title.ratings_{{ ds }}.tsv.gz',
    dag=dag,
)

download_title_basics = HttpDownloadOperator(
    task_id='download_title_basics',
    download_uri='https://datasets.imdbws.com/title.basics.tsv.gz',
    save_to='/home/airflow/imdb/title.basics_{{ ds }}.tsv.gz',
    dag=dag,
)

unzip_title_ratings = UnzipFileOperator(
    task_id='unzip_title_ratings',
    zip_file='home/airflow/imdb/title.ratings_{{ ds }}.tsv.gz',
    extract_to='/home/airflow/imdb/title.ratings_{{ ds }}.tsv',
    dag=dag,
)

unzip_title_basics = UnzipFileOperator(
    task_id='unzip_title_basics',
    zip_file='home/airflow/imdb/title.basics_{{ ds }}.tsv.gz',
    extract_to='/home/airflow/imdb/title.basics_{{ ds }}.tsv',
    dag=dag,
)

create_hdfs_imdb_import_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_imdb_dir',
    directory='/user/hadoop/imdb',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

create_hdfs_title_ratings_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_title_ratings_dir',
    directory='/user/hadoop/imdb/title_ratings',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

create_hdfs_title_basics_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_title_basics_dir',
    directory='/user/hadoop/imdb/title_basics',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

hdfs_put_title_ratings = HdfsPutFileOperator(
    task_id='upload_title_ratings_to_hdfs',
    local_file='/home/airflow/imdb/title.ratings_{{ ds }}.tsv',
    remote_file='/user/hadoop/imdb/title_ratings/title.ratings_{{ ds }}.tsv',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

hdfs_put_title_basics = HdfsPutFileOperator(
    task_id='upload_title_basics_to_hdfs',
    local_file='/home/airflow/imdb/title.basics_{{ ds }}.tsv',
    remote_file='/user/hadoop/imdb/title_basics/title.basics_{{ ds }}.tsv',
    hdfs_conn_id='hdfs_default',
    dag=dag,
)

create_local_import_dir >> clear_local_import_dir 
clear_local_import_dir >> download_title_ratings >> unzip_title_ratings >> create_hdfs_imdb_import_dir >> create_hdfs_title_ratings_dir >> hdfs_put_title_ratings
clear_local_import_dir >> download_title_basics >> unzip_title_basics >> create_hdfs_imdb_import_dir >> create_hdfs_title_basics_dir >> hdfs_put_title_basics

