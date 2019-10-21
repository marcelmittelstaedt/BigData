CREATE EXTERNAL TABLE IF NOT EXISTS name_basics_partitioned (
		nconst STRING, 
		primary_name STRING, 
		birth_year INT, 
		death_year STRING, 
		primary_profession STRING, 
		known_for_titles STRING
) PARTITIONED BY (partition_is_alive STRING) 
STORED AS ORCFILE LOCATION '/user/hadoop/imdb/actors_partitioned';