CREATE EXTERNAL TABLE IF NOT EXISTS imdb_actors(
	nconst STRING, 
	primary_name STRING, 
	birth_year INT, 
	death_year STRING, 
	primary_profession STRING, 
	known_for_titles STRING
	) COMMENT 'IMDb Actors' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/actors';
