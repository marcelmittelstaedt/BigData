#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Initialize SparkContext and SparkSession
import findspark, os
findspark.init('/home/hadoop/spark')
import pyspark
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setMaster("yarn").setAppName("Jupyter PySpark Test")
sc = pyspark.SparkContext(conf = conf)
spark = SparkSession(sc)


# In[2]:


# Execute Word Count Example
text_file = spark.read.text("/user/hadoop/Faust_1.txt").rdd.map(lambda r: r[0])
words = text_file.flatMap(lambda line: line.split(" "))
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
output = counts.collect()
sorted_output = sorted(output,key=lambda x:(-x[1],x[0]))
print(sorted_output[:10])

