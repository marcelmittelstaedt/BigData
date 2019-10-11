CREATE EXTERNAL TABLE IF NOT EXISTS imdb_ratings(
	tconst STRING, 
	average_rating DECIMAL(2,1), 
	num_votes BIGINT
) COMMENT ‚IMDb Ratings‘ ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/ratings‘;
