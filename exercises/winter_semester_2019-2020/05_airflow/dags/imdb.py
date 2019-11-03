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
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}

hiveSQL_create_table_title_ratings='''
CREATE EXTERNAL TABLE IF NOT EXISTS title_ratings(
	tconst STRING,
        average_rating DECIMAL(2,1),
        num_votes BIGINT
) COMMENT 'IMDb Ratings' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_ratings'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_create_table_title_basics='''
CREATE EXTERNAL TABLE IF NOT EXISTS title_basics(
	tconst STRING,
	title_type STRING,
	primary_title STRING,
	original_title STRING,
	is_adult DECIMAL(1,0),
	start_year DECIMAL(4,0),
	end_year STRING,
	runtime_minutes INT,
	genres STRING
) COMMENT 'IMDb Movies' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_add_partition_title_ratings='''
ALTER TABLE title_ratings
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/';
'''

hiveSQL_add_partition_title_basics='''
ALTER TABLE title_basics
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/imdb/title_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/';
'''

hiveSQL_create_top_tvseries_external_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS top_tvseries (
    original_title STRING, 
    start_year DECIMAL(4,0), 
    end_year STRING,  
    average_rating DECIMAL(2,1), 
    num_votes BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb_final/top_tvseries';
'''

hiveQL_create_top_movies_external_table='''
CREATE TABLE IF NOT EXISTS top_movies (
    original_title STRING, 
    start_year DECIMAL(4,0), 
    average_rating DECIMAL(2,1), 
    num_votes BIGINT
) STORED AS ORCFILE LOCATION '/user/hadoop/imdb_final/top_movies';
'''

hiveSQL_insertoverwrite_top_movies_table='''
INSERT OVERWRITE TABLE top_movies
SELECT
    m.original_title,
    m.start_year,
    r.average_rating,
    r.num_votes
FROM
    title_basics m
    JOIN title_ratings r ON (m.tconst = r.tconst)
WHERE
    m.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and m.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and m.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and r.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and r.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.num_votes > 200000 AND r.average_rating > 8.6
    AND m.title_type = 'movie' AND m.start_year > 2000
'''

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

create_hdfs_title_ratings_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_title_ratings_dir',
    directory='/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_hdfs_title_basics_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_title_basics_dir',
    directory='/user/hadoop/imdb/title_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_title_ratings = HdfsPutFileOperator(
    task_id='upload_title_ratings_to_hdfs',
    local_file='/home/airflow/imdb/title.ratings_{{ ds }}.tsv',
    remote_file='/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/title.ratings_{{ ds }}.tsv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_title_basics = HdfsPutFileOperator(
    task_id='upload_title_basics_to_hdfs',
    local_file='/home/airflow/imdb/title.basics_{{ ds }}.tsv',
    remote_file='/user/hadoop/imdb/title_basics/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/title.basics_{{ ds }}.tsv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_HiveTable_title_ratings = HiveOperator(
    task_id='create_title_ratings_table',
    hql=hiveSQL_create_table_title_ratings,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_title_basics = HiveOperator(
    task_id='create_title_basics_table',
    hql=hiveSQL_create_table_title_basics,
    hive_cli_conn_id='beeline',
    dag=dag)

addPartition_HiveTable_title_ratings = HiveOperator(
    task_id='add_partition_title_ratings_table',
    hql=hiveSQL_add_partition_title_ratings,
    hive_cli_conn_id='beeline',
    dag=dag)

addPartition_HiveTable_title_basics = HiveOperator(
    task_id='add_partition_title_basics_table',
    hql=hiveSQL_add_partition_title_basics,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

pyspark_top_tvseries = SparkSubmitOperator(
    task_id='pyspark_write_top_tvseries_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_top_tvseries.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_calculate_top_tvseries',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/imdb', '--hdfs_target_dir', '/user/hadoop/imdb_final/top_tvseries', '--hdfs_target_format', 'csv'],
    dag = dag
)

create_table_for_top_tvseries = HiveOperator(
    task_id='create_top_tvseries_external_table',
    hql=hiveSQL_create_top_tvseries_external_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_top_movies = HiveOperator(
    task_id='create_top_movies_external_table',
    hql=hiveQL_create_top_movies_external_table,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_insert_overwrite_top_movies = HiveOperator(
    task_id='hive_write_top_movies_table',
    hql=hiveSQL_insertoverwrite_top_movies_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_local_import_dir >> clear_local_import_dir 
clear_local_import_dir >> download_title_ratings >> unzip_title_ratings >> create_hdfs_title_ratings_partition_dir >> hdfs_put_title_ratings >> create_HiveTable_title_ratings >> addPartition_HiveTable_title_ratings >> dummy_op
clear_local_import_dir >> download_title_basics >> unzip_title_basics >> create_hdfs_title_basics_partition_dir >> hdfs_put_title_basics >> create_HiveTable_title_basics >> addPartition_HiveTable_title_basics >> dummy_op
dummy_op >> pyspark_top_tvseries >> create_table_for_top_tvseries
dummy_op >> create_HiveTable_top_movies >> hive_insert_overwrite_top_movies

