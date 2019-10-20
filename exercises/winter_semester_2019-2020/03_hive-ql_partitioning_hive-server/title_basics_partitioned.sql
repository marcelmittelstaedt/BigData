CREATE TABLE IF NOT EXISTS title_basics_partitioned (
	tconst STRING, 
	title_type STRING, 
	primary_title STRING, 
	original_title STRING, 
	is_adult DECIMAL(1,0), 
	start_year DECIMAL(4,0), 
	end_year STRING,  
	runtime_minutes INT, 
	genres STRING
) PARTITIONED BY (partition_year DECIMAL(4,0)) STORED AS ORCFILE LOCATION '/user/hadoop/imdb/title_basics_partitioned';