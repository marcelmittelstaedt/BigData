CREATE TABLE `default.imdb_ratings_final`(
  `tconst` string, 
  `average_rating` decimal(2,1), 
  `num_votes` bigint)
PARTITIONED BY ( 
  `partition_quality` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://localhost:9000/user/hadoop/imdb_final/title_ratings'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1539198576');
