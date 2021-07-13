# Databricks notebook source
# first, copy my cdc data from my datalake to yours
filepath = f"{userhome}/cdc_raw"

dbutils.fs.rm(filepath,True)

spark.sql("DROP TABLE IF EXISTS cdc_raw;")
spark.sql(f""" 
  CREATE TABLE cdc_raw
  DEEP CLONE delta.`{mount_location_stream_upsert}/pii/raw`
  LOCATION '{filepath}'
""")

# COMMAND ----------

bronzePath = userhome + "/bronze"

class FileArrival:
    def __init__(self, path, reset=True, max_batch=3):
        self.rawDF = spark.read.format("delta").load(filepath)
        self.path = path
        self.batch = 1
        self.max_batch = max_batch
        if reset == True:
            dbutils.fs.rm(self.path, True)
            
    def arrival(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= max_batch:
                (self.rawDF.filter(F.col("batch") == self.batch)
                    .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                    .write
                    .mode("append")
                    .format("delta")
                    .save(self.path))
                self.batch += 1
        else:
            (self.rawDF.filter(F.col("batch") == self.batch)
                .select('mrn','dob','sex','gender','first_name','last_name','street_address','zip','city','state','updated')
                .write
                .mode("append")
                .format("delta")
                .save(self.path))
            self.batch += 1

sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

Bronze = FileArrival(bronzePath)

silverPath = userhome + "/silver"

dbutils.fs.rm(silverPath,True)
spark.sql("DROP TABLE IF EXISTS silver;")
spark.sql(f"""
    CREATE TABLE silver
    DEEP CLONE delta.`{mount_location_stream_upsert}/pii/silver`
    LOCATION '{silverPath}'
""")


# COMMAND ----------

print(f"""
vars used in upsert/streaming lab:
username: {username}
userhome: {userhome}
database: {database}
mount_location_stream_upsert: {mount_location_stream_upsert}
filepath:  {filepath} (this is where we copy the mount_location_stream_upsert)
bronzePath: {bronzePath}
silverPath: {silverPath}  (this is where we copy my primed silver data)
""")
