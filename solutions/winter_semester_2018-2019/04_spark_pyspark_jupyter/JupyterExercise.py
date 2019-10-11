#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
import os
findspark.init('/home/hadoop/spark')
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
conf = SparkConf().setAppName('Jupyter PySpark Exercise')
conf.set('spark.yarn.dist.files','file:/home/hadoop/spark/python/lib/pyspark.zip,file:/home/hadoop/spark/python/lib/py4j-0.10.7-src.zip')
conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.10.7-src.zip')
conf.setMaster('yarn') # Run on Yarn, not local
sc = SparkContext(conf=conf)


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession(sc)


# In[24]:


imdb_movie_dataframe = spark.read.format('com.databricks.spark.csv').options(delimiter = '\t',header ='true',nullValue ='null',inferSchema='true').load('hdfs:///user/hadoop/imdb_raw/title_basics/2018/12/7/title.basics.tsv')


# In[5]:


imdb_movie_dataframe.show(5)


# In[9]:


from pyspark.sql.functions import col
movie_count = imdb_movie_dataframe.filter(col('titleType') == 'movie').count()


# In[11]:


print movie_count


# In[13]:


imdb_people_dataframe = spark.read.format('com.databricks.spark.csv').options(delimiter = '\t',header ='true',nullValue ='null',inferSchema='true').load('hdfs:///user/hadoop/names.basics.tsv')


# In[19]:


oldest_one = imdb_people_dataframe.filter(col('birthYear') != '\N').sort(col("birthYear").asc()).show(3)


# In[20]:


imdb_ratings_dataframe = spark.read.format('com.databricks.spark.csv').options(delimiter = '\t',header ='true',nullValue ='null',inferSchema='true').load('hdfs:///user/hadoop/imdb_raw/title_ratings/2018/12/7/title.ratings.tsv')


# In[25]:


all_imdb_dataframe = imdb_ratings_dataframe.join(imdb_movie_dataframe, imdb_ratings_dataframe.tconst == imdb_movie_dataframe.tconst)


# In[34]:


good_movies = all_imdb_dataframe.filter(col('numVotes') > 100000).filter(col('startYear') > 2000).filter(col('averageRating') > 8.0).filter(col('titleType') == 'movie')


# In[35]:


good_movies.sort(col("averageRating").desc(),col("numVotes").desc()).select("originalTitle", "startYear", "averageRating", "numVotes").show(5)


# In[36]:


good_movies.sort(col("averageRating").desc(),col("numVotes").desc()).select("originalTitle", "startYear", "averageRating", "numVotes").write.format("csv").save("hdfs:///user/hadoop/good_movies") 
# saved on HDFS as /user/hadoop/good_movies/part-00000-f01c02b6-516a-4a28-9b7e-d271bfc47536-c000.csv
...


# In[37]:


print good_movies.count()


# In[38]:


plot_dataframe = good_movies.groupBy('startYear').count().sort(col("startYear").asc())


# In[39]:


plot_dataframe.show()


# In[40]:


import matplotlib.pyplot as plt
import pandas
pd_df=plot_dataframe.select("startYear", "count").toPandas()


# In[41]:


pd_df.plot.bar(x='startYear',y='count')

