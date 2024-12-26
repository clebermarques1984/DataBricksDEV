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
df_roads = utils.readstream_from_table(spark, catalog, input_database, 'raw_roads')

# apply clear transformations
#df_roads_clean = utils.apply_clear_tr(df_roads)

# apply specific transformations
print('Creating silver_roads columns : ', end='')
df_roads_tr = (df_roads_clean
               .withColumn("Road_Category_Name",
                                         when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                                         .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                                         .when(col('Road_Category') == 'PA','Class A Principal road')
                                         .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                                         .when(col('Road_Category') == 'M','Class B road')
                                         .otherwise('NA'))
               ## Creating road_type column
               .withColumn("Road_Type",
                           when(col('Road_Category_Name').like('%Class A%'),'Major')
                           .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                           .otherwise('NA'))
                ## Creating transformed time column
                .withColumn('Transformed_Time',
                            current_timestamp())
               )
print('Success!! ')

# read table
df_traffic = utils.readstream_from_table(spark, catalog, input_database, 'raw_traffic')

# apply clear transformations
df_traffic_clean = utils.apply_clear_tr(df_traffic)

# apply specific transformations
print('Creating silver_traffic columns : ', end='')
df_traffic_tr = (df_traffic_clean
    ## Getting count of electric vehicles
    .withColumn('Electric_Vehicles_Count',
                col('EV_Car') + col('EV_Bike'))
    ## Getting count of all motor vehicles
    .withColumn('Motor_Vehicles_Count',
                col('Electric_Vehicles_Count') +
                col('Two_wheeled_motor_vehicles') +
                col('Cars_and_taxis') +
                col('Buses_and_coaches') +
                col('LGV_Type') +
                col('HGV_Type'))
    ## Creating transformed time column
    .withColumn('Transformed_Time',
                current_timestamp())
    )
print('Success!! ')

# Writing to tables
utils.write_stream_to_table(spark, df_roads_tr, catalog, database, 'silver_roads')
utils.write_stream_to_table(spark, df_traffic_tr, catalog, database, 'silver_traffic')

