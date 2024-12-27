class Ingestor:
    def _init__(self, path, file_format, catalog, schema):
        self.path = path
        self.file_format = file_format
        self.catalog = catalog
        self.schema = schema

    def load(self):
        df = (spark.read
            .format(self.file_format)
            #.option("header", "true")
            .load(self.path))
        return df

    def save(self, df, table):
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{table}"))
    
    def run(self):
        df = self.load()
        self.save(df)