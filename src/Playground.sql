-- Databricks notebook source
-- MAGIC %python
-- MAGIC path_raw_roads = '/Volumes/ws_databricks_dev/default/raw_roads'
-- MAGIC
-- MAGIC df = (spark.read
-- MAGIC       .format("csv")
-- MAGIC       .option("header", "true")
-- MAGIC       .load(path_raw_roads))
-- MAGIC
-- MAGIC (df.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable("dev_catalog.bronze.bronze_roads"))

-- COMMAND ----------

-- Count of Bronze Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_traffic

-- COMMAND ----------

-- Count of Bronze Roads Rows

SELECT COUNT(*) FROM `dev_catalog`.`bronze`.raw_roads

-- COMMAND ----------

-- Count of Silver Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_traffic

-- COMMAND ----------

-- Count of Silver Roads Rows

SELECT COUNT(*) FROM `dev_catalog`.`silver`.silver_roads

-- COMMAND ----------

-- Count of Gold Traffic Rows

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_traffic

-- COMMAND ----------

-- Count of Gold Road Rows

SELECT COUNT(*) FROM `dev_catalog`.`gold`.gold_roads
