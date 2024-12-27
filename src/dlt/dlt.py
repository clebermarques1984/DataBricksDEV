# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE raw_traffic_dl
# MAGIC AS SELECT 
# MAGIC "Record ID"   AS  Record_ID ,
# MAGIC "Count point id"    AS  Count_point_id ,
# MAGIC "Direction of travel"     AS Direction_of_travel  ,
# MAGIC "Year"     AS  Year ,
# MAGIC "Count date"     AS  Count_date ,
# MAGIC "hour"     AS  hour ,
# MAGIC "Region id"     AS   Region_id,
# MAGIC "Region name"     AS   Region_name,
# MAGIC "Local authority name"     AS  Local_authority_name ,
# MAGIC "Road name"     AS  Road_name ,
# MAGIC "Road Category ID"     AS  Road_Category_ID ,
# MAGIC "Start junction road name"     AS  Start_junction_road_name ,
# MAGIC "End junction road name"     AS   End_junction_road_name,
# MAGIC "Latitude"     AS   Latitude,
# MAGIC "Longitude"     AS   Longitude,
# MAGIC "Link length km"     AS  Link_length_km ,
# MAGIC "Pedal cycles"     AS   Pedal_cycles ,
# MAGIC "Two wheeled motor vehicles"     AS  Two_wheeled_motor_vehicles ,
# MAGIC "Cars and taxis"     AS  Cars_and_taxis ,
# MAGIC "Buses and coaches"     AS   Buses_and_coaches,
# MAGIC "LGV Type"     AS   LGV_Type,
# MAGIC "HGV Type"     AS  HGV_Type ,
# MAGIC "EV Car"     AS   EV_Car,
# MAGIC "EV Bike" AS EV_Bike
# MAGIC
# MAGIC FROM STREAM(read_files(
# MAGIC     'abfss://landing@databricksdevstg.dfs.core.windows.net/raw_traffic',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST'
# MAGIC ))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE raw_roads_dl
# MAGIC AS SELECT 
# MAGIC "Road ID"    AS   Road_ID  ,
# MAGIC "Road category id"    AS    Road_category_id ,
# MAGIC "Road category"    AS   Road_category  ,
# MAGIC "Region id"    AS     Region_id,
# MAGIC "Region name"    AS     Region_name,
# MAGIC "Total link length km"    AS    Total_link_length_km ,
# MAGIC "Total link length miles"    AS    Total_link_length_miles ,
# MAGIC "All motor vehicles"   AS   All_motor_vehicles
# MAGIC
# MAGIC FROM STREAM(read_files(
# MAGIC     'abfss://landing@databricksdevstg.dfs.core.windows.net/raw_roads',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     mode => 'FAILFAST'
# MAGIC ))
