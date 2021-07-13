# Databricks notebook source

import pyspark.sql.functions as F
import re

# vars to change
lab = "intro"
username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{lab}"
database = f"""{lab}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""
mount_location = f"/mnt/mtc-workshop/{lab}/"
container = "datasets"
storage_account = "davewdemoblobs"
folder = "definitive-guide/data/activity-json/streaming"
token = "sp=rl&st=2020-07-01T17:38:50Z&se=2031-07-02T01:38:50Z&spr=https&sv=2020-08-04&sr=c&sig=uV%2BQtIrBlgE6Y5xQWEZRKlSTAJCzThnrdaSMfAA0dIo%3D"

print(f"""
vars used in intro lab:
username: {username}
userhome: {userhome}
database: {database}
mount_location: {mount_location}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

# COMMAND ----------

def mount_blob_using_sas(storage_account, container, mount_location,folder, token): 
  
  if any(mount.mountPoint == mount_location for mount in dbutils.fs.mounts()):
    print(f"found {mount_location}. Unmounting, just in case.")
    dbutils.fs.unmount(mount_location) 
  
  confkey = "fs.azure.sas.%s.%s.blob.core.windows.net" % (container,storage_account)
  try:
    dbutils.fs.mount( 
      source = "wasbs://%s@%s.blob.core.windows.net/%s" % (container,storage_account,folder),
      mount_point = mount_location, 
      extra_configs = {confkey:token}
    )
  except Exception as e: 
    print(e)

# COMMAND ----------

mount_blob_using_sas(storage_account,container,mount_location,folder,token)

# COMMAND ----------

# this is for lakehouse pipelines
folder = "healthcare"
token = "sp=rl&st=2020-07-01T17:38:50Z&se=2031-07-02T01:38:50Z&spr=https&sv=2020-08-04&sr=c&sig=uV%2BQtIrBlgE6Y5xQWEZRKlSTAJCzThnrdaSMfAA0dIo%3D"
mount_location_stream = f"/mnt/mtc-workshop/{folder}/"
mount_blob_using_sas(storage_account,container,mount_location_stream,folder,token)

# COMMAND ----------

print(f"""
vars used in healthcare/lakehouse/streaming lab:
username: {username}
userhome: {userhome}
database: {database}
mount_location_stream: {mount_location_stream}""")

# COMMAND ----------

# this is for streaming upserts
folder = "v01"
token = "sp=rl&st=2020-07-01T17:38:50Z&se=2031-07-02T01:38:50Z&spr=https&sv=2020-08-04&sr=c&sig=uV%2BQtIrBlgE6Y5xQWEZRKlSTAJCzThnrdaSMfAA0dIo%3D"
mount_location_stream_upsert = f"/mnt/mtc-workshop/streaming-source/"
mount_blob_using_sas(storage_account,container,mount_location_stream_upsert,folder,token)

# COMMAND ----------

print(f"""
vars used in healthcare/lakehouse/streaming lab:
username: {username}
userhome: {userhome}
database: {database}
mount_location_stream_upsert: {mount_location_stream_upsert}""")

# COMMAND ----------

#display(dbutils.fs.ls(mount_location_stream))

# COMMAND ----------


