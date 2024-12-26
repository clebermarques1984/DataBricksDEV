# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys

sys.path.insert(0, "../lib/")

import lib

catalog = "dev_catalog"
database = "bronze"

