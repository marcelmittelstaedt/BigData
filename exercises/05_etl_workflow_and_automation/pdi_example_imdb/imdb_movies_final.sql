CREATE TABLE `default.imdb_movies_final`(
  `tconst` string, 
  `title_type` string, 
  `primary_title` string, 
  `original_title` string, 
  `is_adult` decimal(1,0), 
  `start_year` decimal(4,0), 
  `end_year` string, 
  `runtime_minutes` int, 
  `genres` string)
PARTITIONED BY ( 
  `partition_year` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://localhost:9000/user/hadoop/imdb_final/title_basics'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1539197414');
