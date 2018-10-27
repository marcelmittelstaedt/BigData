CREATE TABLE `default.word_counts`(
  `word` string, 
  `count` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse/word_counts'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'transient_lastDdlTime'='1540665111');
