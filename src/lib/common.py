from pyspark.sql import *

def import_query(path):
    """Imports a SQL query from a file and returns the query as a string"""
    with open(path, "r") as open_file:
        return open_file.read()

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

def create_schema(spark, catalog, path, name):
    """Create a schema in the given catalog"""
    print(f'Using {catalog} Catalog')
    spark.sql(f""" USE CATALOG '{catalog}'""")
    print(f'Creating {name} Schema in {catalog}')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{name}` MANAGED LOCATION '{path}/{name}'""")
    print("************************************")

def create_table(spark, catalog, schema, name):
    """Create a table in the given schema"""
    query = import_query(f"{name}.sql")
    print(f'Creating table {name} in {catalog}.{schema}')
    spark.sql(query.format(catalog=catalog, schema=schema, tablename=name))
    print("************************************")
