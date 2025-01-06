# Databricks notebook source
dbutils.widgets.text(name="env",defaultValue='dev',label='Environment in lower case')
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
ws_rootpath = utils.get_external_path(spark, 'wscmf') + env

# local functions
def extract_table(tablename):
    df = utils.readstream_from_csv(spark, tablename, ws_rootpath)
    df1 = df.withColumn('Extract_Time', current_timestamp())
    utils.write_stream_to_table(spark, df1, catalog, database, tablename, ws_rootpath)

# create schema
utils.create_schema(spark, catalog, database, ws_rootpath)

# extract tables
extract_table('raw_roads')
extract_table('raw_traffic')
