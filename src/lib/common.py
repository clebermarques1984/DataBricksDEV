import json
from pyspark.sql import types
from pyspark.sql import *

def import_query(path):
    """Imports a SQL query from a file and returns the query as a string"""
    with open(path, "r") as open_file:
        return open_file.read()

def import_schema(tablename):
    with open(f"{tablename}.json", "r") as open_file:
        schema_json = json.load(open_file) ## dicionário
    schema_df = types.StructType.fromJson(schema_json)
    return schema_df

def handle_NULLs(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')
    print('***********************')
    return df_numeric

def get_external_path(spark, path):
    """Get the external path from a given path"""
    return spark.sql(f"describe external location `{path}`").select("url").collect()[0][0]

def create_schema(spark, catalog, schemaname):
    path = get_external_path(spark, f'{schemaname}')
    """Create a schema in the given catalog"""
    print(f'Using {catalog} Catalog')
    spark.sql(f""" USE CATALOG '{catalog}'""")
    print(f'Creating {schemaname} Schema in {catalog}')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{schemaname}` MANAGED LOCATION '{path}/{schemaname}'""")
    print("************************************")

def create_table(spark, catalog, database, name):
    """Create a table in the given database"""
    query = import_query(f"{name}.sql")
    print(f'Creating table {name} in {catalog}.{database}')
    spark.sql(query.format(catalog=catalog, schema=database, tablename=name))
    print("************************************")

def readstream_from_csv(spark, tablename):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    """Read the csv data from the landing folder"""
    checkpoints = get_external_path(spark, 'checkpoints')
    landing = get_external_path(spark, 'landing')
    schema = import_schema(tablename)
    print(f"Reading {tablename} Data:  ", end='')
    read_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{checkpoints}/{tablename}Load/schemaInfer')
        .option('header','true')
        .schema(schema)
        .load(f'{landing}/{tablename}/')
        )
    print('Read Success')
    print('*******************')
    return read_stream

def write_stream_to_table(spark, stream, catalog, database, tablename):
    """Write the stream to the target table"""
    checkpoints = get_external_path(spark, 'checkpoints')
    print(f'Writing data to {catalog} {tablename} table: ', end='' )
    write_stream = (stream.writeStream
                    .format('delta')
                    .option("checkpointLocation",f'{checkpoints}/{tablename}Load/Checkpt')
                    .outputMode('append')
                    .queryName('{tablename}WriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{catalog}`.`{database}`.`{tablename}`"))
    
    write_stream.awaitTermination()
    print('Write Success')
    print("****************************")

def extract_table(spark, catalog, database, tablename):
    stream = readstream_from_csv(spark, tablename)
    write_stream_to_table(spark, stream, catalog, database, tablename)
