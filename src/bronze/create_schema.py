# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

#imports
import sys
sys.path.insert(0, "../lib/")
import common as utils

#variable
catalog = "dev_catalog"
database = "bronze"

#create schema
utils.create_schema(spark, catalog, database)

#create tables
utils.create_table(spark, catalog, database, 'raw_roads')
utils.create_table(spark, catalog, database, 'raw_traffic')
