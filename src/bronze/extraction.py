# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

# imports
import sys
sys.path.insert(0, "../lib/")
import common as utils
from pyspark.sql.functions import current_timestamp

# variable
catalog = f'{env}_catalog'
database = "bronze"

# local functions
def extract_table(tablename):
    df = utils.readstream_from_csv(spark, tablename)
    df1 = df.withColumn('Extract_Time', current_timestamp())
    utils.write_stream_to_table(spark, df1, catalog, database, tablename)

# create schema
utils.create_schema(spark, catalog, database)

# extract tables
extract_table('raw_roads')
extract_table('raw_traffic')
