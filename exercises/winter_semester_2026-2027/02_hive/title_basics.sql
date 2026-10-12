CREATE EXTERNAL TABLE IF NOT EXISTS title_basics (
	tconst STRING,
	title_type STRING,
	primary_title STRING,
	original_title STRING,
	is_adult DECIMAL(1,0),
	start_year DECIMAL(4,0),
	end_year STRING,
	runtime_minutes INT,
	genres STRING
) COMMENT 'IMDb Movies' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/title_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
