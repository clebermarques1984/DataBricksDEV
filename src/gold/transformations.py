# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# imports
import sys
sys.path.insert(0, "../lib/")
import common as utils
from pyspark.sql.functions import when, col
from pyspark.sql.functions import current_timestamp

# variable
catalog = f'{env}_catalog'
input_database = "silver"
database = "gold"

# create schema
utils.create_schema(spark, catalog, database)

# read table
df_roads = utils.readstream_from_table(spark, catalog, input_database, 'silver_roads')

# apply specific transformations
print('Creating gold_roads columns : ', end='')
df_roads_tr = (df_roads
               ## Creating load time column
               .withColumn('Load_Time',
                            current_timestamp())
               )
print('Success!! ')

# read table
df_traffic = utils.readstream_from_table(spark, catalog, input_database, 'silver_traffic')

# apply specific transformations
print('Creating gold_traffic silver columns : ', end='')
df_traffic_tr = (df_traffic
    ## Creating vehicle Intensity Column
    .withColumn('Vehicle_Intensity',
                col('Motor_Vehicles_Count') / col('Link_length_km'))
    ## Creating load time column
    .withColumn('Load_Time',
                current_timestamp())
    )
print('Success!! ')

# Writing to tables
utils.write_stream_to_table(spark, df_roads_tr, catalog, database, 'gold_roads')
utils.write_stream_to_table(spark, df_traffic_tr, catalog, database, 'gold_traffic')

