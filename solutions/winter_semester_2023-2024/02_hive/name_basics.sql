CREATE EXTERNAL TABLE IF NOT EXISTS name_basics (
	nconst STRING, 
	primary_name STRING, 
	birth_year INT, 
	death_year STRING, 
	primary_profession STRING, 
	known_for_titles STRING
	) COMMENT 'IMDb Actors' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/name_basics'
TBLPROPERTIES ('skip.header.line.count'='1');