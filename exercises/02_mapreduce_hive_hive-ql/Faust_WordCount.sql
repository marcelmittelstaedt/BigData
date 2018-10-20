CREATE TABLE faust (line STRING); 

LOAD DATA INPATH '/user/hadoop/faust' OVERWRITE INTO TABLE faust;
 
CREATE TABLE word_counts AS 
SELECT word, count(1) AS count FROM  
(SELECT explode(split(line, '\\s')) AS word FROM faust) temp 
 GROUP BY word ORDER BY word;
