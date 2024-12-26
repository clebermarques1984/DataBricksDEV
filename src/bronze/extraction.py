# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue='',label='Enter the environment in lower case')
env = dbutils.widgets.get("env")

# COMMAND ----------

#imports
import sys
sys.path.insert(0, "../lib/")
import common as utils

#variable
catalog = f'{env}_catalog'
database = "bronze"

#extract tables
utils.extract_table(spark, catalog, database, 'raw_roads')
utils.extract_table(spark, catalog, database, 'raw_traffic')

