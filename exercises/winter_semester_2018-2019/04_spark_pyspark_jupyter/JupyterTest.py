#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
import os
findspark.init('/home/hadoop/spark')
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
conf = SparkConf().setAppName('Jupyter PySpark Test')
conf.set('spark.yarn.dist.files','file:/home/hadoop/spark/python/lib/pyspark.zip,file:/home/hadoop/spark/python/lib/py4j-0.10.7-src.zip')
conf.setExecutorEnv('PYTHONPATH','pyspark.zip:py4j-0.10.7-src.zip')
conf.setMaster('yarn') # Run on Yarn, not local
sc = SparkContext(conf=conf)


# In[2]:


dummy_data = range(1, 100)
dummy_rdd = sc.parallelize(dummy_data)
print(dummy_rdd.filter(lambda x: x<10).collect())


# In[2]:


from pyspark.sql import SparkSession
spark = SparkSession(sc)


# In[5]:


imdb_ratings_dataframe = spark.read.format('com.databricks.spark.csv').options(delimiter = '\t',header ='true',nullValue ='null',inferSchema='true').load('hdfs:///user/hadoop/imdb_raw/title_ratings/2018/12/7/title.ratings.tsv')


# In[5]:


imdb_ratings_dataframe.show(5) # show first 5 lines of tsv file (now a spark dataframe)


# In[6]:


imdb_ratings_dataframe.printSchema() # show schema of tsv file (now dataframe)


# In[8]:


imdb_ratings_dataframe.count() # show number of rows within dataframe


# In[6]:


from pyspark.sql.functions import col, avg
imdb_ratings_dataframe.agg(avg(col("averageRating"))).show() # calculate average movie rating 


# In[11]:


imdb_ratings_dataframe.filter(col('averageRating') > 9.5).show(5) # filter movie ratings > 9.5 and show first 5


# In[16]:


imdb_ratings_dataframe.write.format("csv").save("hdfs:///user/hadoop/imdb_ratings") 
# saved on HDFS as /user/hadoop/imdb_ratings/part-00000-ce6e8310-64c0-4fb9-b597-d66a92f5309b-c000.csv


# In[7]:


imdb_movies_dataframe = spark.read.format('com.databricks.spark.csv').options(delimiter = '\t',header ='true',nullValue ='null',inferSchema='true').load('hdfs:///user/hadoop/imdb_raw/title_basics/2018/12/7/title.basics.tsv')


# In[18]:


all_imdb_dataframe = imdb_ratings_dataframe.join(imdb_movies_dataframe, imdb_ratings_dataframe.tconst == imdb_movies_dataframe.tconst)


# In[19]:


all_imdb_dataframe.select("primaryTitle", "startYear", "averageRating", "numVotes").show(5)


# In[25]:


plot_dataframe = imdb_movies_dataframe.filter(col('startYear') != '\N').filter(col('startYear') > 1990).filter(col('titleType') == 'movie').groupBy('startYear').count().sort(col("startYear").desc())


# In[30]:


plot_dataframe.show()


# In[27]:


import matplotlib.pyplot as plt
import pandas
pd_df=plot_dataframe.select("startYear", "count").toPandas()


# In[29]:


pd_df.plot.bar(x='startYear',y='count')

