#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import Spark Libraries
import findspark, os
findspark.init('/home/hadoop/spark')
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder     .master("yarn")     .appName("Jupyter/PySpark Exercises")     .enableHiveSupport()     .getOrCreate()


# In[2]:


# Read IMDb title basics CSV file from HDFS
df_title_basics = spark.read     .format('csv')     .options(header='true', delimiter='\t', nullValue='null', inferSchema='true')     .load('/user/hadoop/imdb/title_basics/title.basics.tsv')


# In[3]:


# Print Schema of DataFrame
df_title_basics.printSchema()


# In[4]:


# Print First 3 Rows of DataFrame Data
df_title_basics.show(3)


# In[5]:


# Get Number of Rows of a DataFrame
df_title_basics.count()


# In[6]:


# Groups and Counts: Get column titleTypes values with counts and ordered descending
from pyspark.sql.functions import desc
df_title_basics     .groupBy("titleType")     .count()     .orderBy(desc("count"))     .show()


# In[7]:


# Calculate average Movie length in minutes
from pyspark.sql.functions import avg, col
df_title_basics     .where(col('titleType') == 'movie')     .agg(avg('runtimeMinutes'))     .show() 


# In[8]:


# Save Dataframe back to HDFS (partitioned) as Parquet files
df_title_basics.repartition('startYear').write     .format("parquet")     .mode("overwrite")     .partitionBy('startYear')     .save('/user/hadoop/imdb/title_basics_partitioned_files')


# In[9]:


# Save Dataframe back to HDFS (partitioned) as EXTERNAL TABLE and Parquet files
df_title_basics.repartition('startYear').write     .format("parquet")     .mode("overwrite")     .option('path', '/user/hadoop/imdb/title_basics_partitioned_table')     .partitionBy('startYear')     .saveAsTable('default.title_basics_partitioned')


# In[10]:


spark.sql('SHOW TABLES').show(10, False)


# In[11]:


# Read External Spark table in programmatical way
df = spark.table('default.title_basics_partitioned')     .where(col('startYear') == '2020')     .select('tconst', 'primaryTitle', 'startYear')
# Print Result
df.show(3)


# In[12]:


# Read External Spark table using plain Spark SQL
df = spark.sql('SELECT tconst, primaryTitle, startYear FROM default.title_basics_partitioned WHERE startYear = 2020')
# Print Result
df.show(3)


# In[13]:


# Read title.ratings.tsv into Spark dataframe
df_title_ratings = spark.read     .format('csv')     .options(header='true', delimiter='\t', nullValue='null', inferSchema='true')     .load('/user/hadoop/imdb/title_ratings/title.ratings.tsv')


# In[14]:


# Print Schema of title_ratings dataframe 
df_title_ratings.printSchema()


# In[15]:


# Show first 3 rows of title_ratings dataframe 
df_title_ratings.show(3)


# In[16]:


# JOIN Data Frames
joined_df = df_title_basics.join(df_title_ratings, df_title_basics.tconst == df_title_ratings.tconst)


# In[17]:


# Print Schema of joined DataFrame
joined_df.printSchema()


# In[18]:


# Show Frist 3 Rows of Joined DataFrame
joined_df.show(3)


# In[19]:


top_tvseries = joined_df     .where(col('titleType') == 'tvSeries')     .where(col('numVotes') > 200000)     .orderBy(desc('averageRating'))     .select('originalTitle', 'startYear', 'endYear', 'averageRating', 'numVotes')

# Print Top 5 TV Series
top_tvseries.show(5)


# In[20]:


from pyspark.sql.functions import when, lit

# Add a calculated column: classify movies as being either 'good' or 'worse' based on average rating
df_with_classification = joined_df     .withColumn('classification', 
                when(col('averageRating') > 8, lit('good')) \
                .otherwise(lit('worse'))) \
    .select('primaryTitle', 'startYear', 'averageRating', 'classification')

# Print Result
df_with_classification.show(3, False)


# In[21]:


# Plot data: good movies per year
import matplotlib.pyplot as plt
import pandas

# Create DataFrame to be plotted
good_movies = df_with_classification     .select('startYear', 'classification')     .where(col('classification') == 'good')     .where(col('startYear') > 2000)     .groupBy('startYear')     .count()     .sort(col('startYear').asc())

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = good_movies.toPandas()

# Plot DataFrame
pandas_df.plot.bar(x='startYear', y='count')

