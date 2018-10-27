CREATE EXTERNAL TABLE `default.imdb_ratings_raw`(
  `tconst` string, 
  `average_rating` decimal(2,1), 
  `num_votes` bigint)
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
  'hdfs://localhost:9000/user/hadoop/imdb_raw/title_ratings'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1539195704');
