CREATE TABLE IF NOT EXISTS imdb_movies_and_ratings_partitioned (
	tconst STRING, 
	title_type STRING, 
	primary_title STRING, 
	original_title STRING, 
	is_adult DECIMAL(1,0), 
	start_year DECIMAL(4,0), 
	end_year STRING,  
	runtime_minutes INT, 
	genres STRING, 
	average_rating DECIMAL(2,1), 
	num_votes BIGINT
) PARTITIONED BY (partition_year int) STORED AS ORCFILE LOCATION '/user/hadoop/imdb/movies_and_ratings_partitioned';
