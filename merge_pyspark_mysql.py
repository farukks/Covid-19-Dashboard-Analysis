#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataIntegration").getOrCreate()

#read data from mysql 
mysql_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/covid19",
    driver="com.mysql.jdbc.Driver",
    dbtable="full_data",
    user="root",
    password="password").load()
mysql_df2 = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/covid19",
    driver="com.mysql.jdbc.Driver",
    dbtable="vaccinations",
    user="root",
    password="password").load()

#merge data
joined_df = mysql_df.join(mysql_df2, ["date","location"])

#write the data to mysql
joined_df.write.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/covid19",
    driver="com.mysql.jdbc.Driver",
    dbtable="joined_table2",
    user="root",
    password="password").mode("overwrite").save()

