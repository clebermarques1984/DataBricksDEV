# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

#imports
import sys
sys.path.insert(0, "../lib/")
import common as utils

#variable
catalog = "ws_databricks_dev"
database = "bronze"
path = utils.get_external_path(spark, database)

#create schema
utils.create_schema(spark, catalog, path, database)

#create tables
utils.create_table(spark, catalog, database, 'raw_roads')
utils.create_table(spark, catalog, database, 'raw_traffic')
