CREATE TABLE IF NOT EXISTS imdb_ratings_partitioned(
		tconst STRING, 
		average_rating DECIMAL(2,1), 
		num_votes BIGINT
) PARTITIONED BY (partition_quality STRING) 
STORED AS ORCFILE LOCATION '/user/hadoop/imdb/ratings_partitioned';
