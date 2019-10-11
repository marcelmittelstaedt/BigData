CREATE TABLE `default.imdb_movies_raw`(
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
  `partition_year` int, 
  `partition_month` int, 
  `partition_day` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://localhost:9000/user/hadoop/imdb_raw/title_basics'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1539192103');
