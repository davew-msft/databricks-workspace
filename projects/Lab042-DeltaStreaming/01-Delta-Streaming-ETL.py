# Databricks notebook source
# MAGIC %md 
# MAGIC ## Lab 042 IoT Streaming
# MAGIC Let's look at use case where we can simulate streaming IoT devices and process that data via both streaming and batching to different zones of the data lake.  
# MAGIC 
# MAGIC ### About the Setup
# MAGIC 
# MAGIC * You will connect to my "demo data" WASB-based data lake as read only to get to the datasets
# MAGIC * You will stream the data into either your datalake, or we'll just stream it to DBFS to keep the setup simple.  
# MAGIC   * if you have your data lake mounted (maybe to /mnt/lake) then use that
# MAGIC   * you will need to change the data lake paths to match your environment
# MAGIC 
# MAGIC ### Connecting to my davcewdemoblobs WASB data
# MAGIC * TODO:  need to complete the steps
# MAGIC * davewdemoblobs/datasets/bikeSharing

# COMMAND ----------

# mount davewdemoblobs storage
# you'll need to change some parameters below and comment/uncomment code a few times before getting it to work
source_account_name = "davewdemoblobs"
container = "datasets"
read_only_container_sas = "sv=2020-04-08&st=2021-04-15T14%3A59%3A00Z&se=2050-04-17T14%3A59%3A00Z&sr=c&sp=rl&sig=fS29eguEC74TTc01rBs%2BS%2BG1J9v6Q4ZqM%2B0B93yTPjA%3D"
mount_dir = "/mnt"
root_project_folder = "{}/{}/bikeSharing".format(mount_dir,container)
# uri:  https://davewdemoblobs.blob.core.windows.net/datasets?sv=2020-04-08&st=2021-04-15T14%3A59%3A00Z&se=2050-04-17T14%3A59%3A00Z&sr=c&sp=rl&sig=fS29eguEC74TTc01rBs%2BS%2BG1J9v6Q4ZqM%2B0B93yTPjA%3D

# this should be run in any notebook/cluster that is running where you want to see the new mount
#dbutils.fs.refreshMounts()

#dbutils.fs.unmount("/mnt/dbricks-delta")


# COMMAND ----------

# let's look at some sample dbx datasets for bike sharing, we'll use this as the source data for batch ingestion
print(root_project_folder)
display(dbutils.fs.ls(root_project_folder))


# COMMAND ----------

# now mount and prep your datalake
# I'm just going to put this to a temp area in DBFS for this lab, it really doesn't matter
# change your path accordingly
datalake_root_project_path = "/mnt/lake/"

display(dbutils.fs.ls(datalake_root_project_path))

# COMMAND ----------

# data-001 contains hour.csv and day.csv 
# let's load it and look at it

hourlydf = (spark
.read
.option("header", True)
.option("inferSchema", True)
.csv("{}/data-001/hour.csv".format(root_project_folder)))


dailydf = (spark
.read
.option("header", True)
.option("inferSchema", True)
.csv("{}
     /data-001/day.csv".format(root_project_folder)))


# COMMAND ----------

# take a minute to familiarize yourself with the data.  
# both are the same data, just aggregated at different time grains
# ie, there is one extra col...hr
# this is aggregated bikesharing data
display(hourlydf)
display(dailydf)

# COMMAND ----------

# since this is aggregated historical data, let's put it into our data lake at 
# bronze/bikesharing/hourly
# bronze/bikesharing/daily
# as the initial data load
# we want to leverage delta-formatted tables

# write hourly data to source
hourlydf.write.format("delta").mode("overwrite").save("{}/bronze/bikeSharing/hourly".format(datalake_root_project_path))

# write daily data to source
dailydf.write.format("delta").mode("overwrite").save("{}/bronze/bikeSharing/daily".format(datalake_root_project_path))

# COMMAND ----------

# take a look at your data folders using the DBFS utilities or your data lake tools to ensure the delta (parquet) data was written out correctly.  

# now we have the initial batch load of data into our data lake.  We would now setup a scheduled job for hourly and daily that would load the newest data into the delta lake.  

# the next example uses streaming data from a simulated IoT device.  We'll stream it into our bronze area every 5 seconds.  
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from time import sleep

# COMMAND ----------

# this simulated dataset is provided from databricks and is available in every workspace
df = (spark.read.format('json').load("/databricks-datasets/iot/iot_devices.json")
.orderBy(col("timestamp"))
.withColumn("id", monotonically_increasing_id()) )

display(df)

# COMMAND ----------

# it isn't a lot of data, but enough to simulate streaming
df.count()

# COMMAND ----------

## for our simulation we will save our data frame to a table and collect batchs to write out on a cadence
try:
  df = spark.sql("SELECT * FROM stream_data")
  print("Table Exists. Loaded Data.")
except:
  df.write.saveAsTable("stream_data")
  print("Table Created.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream_data

# COMMAND ----------

# use a while loop to append data to our bronze delta table, simulating IoT events
# this isn't true "streaming" but it demonstrates how to do append operations on existing delta tables

# as this is executing, look at the files it is writing out in your destination
# every run will generate another part file

# frequency and batch size of load
current = 0
num_rows = df.count()
batch_size = 1000
frequency = 5

while current < num_rows:
    stream_df = spark.sql("select * from stream_data where id >= {} and id < {}".format(current, current+batch_size))
    stream_df.write.format("delta").mode("append").save("{}/bronze/iot/stream_data".format(datalake_root_project_path))
    print("Streamed IDs between: {} and {}".format(current, current+batch_size-1))
    current = current+batch_size

    sleep(frequency)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next 
# MAGIC 
# MAGIC Leave this notebook running and open up the next notebook where we will read this streaming data as it is being streamed.  
# MAGIC If the above query stops running you can simply rerun the previous cell as many times as need to complete the exercise.  
# MAGIC 
# MAGIC You can also stop the above cell at any time when you are finished
# MAGIC 
# MAGIC The [second notebook can be found here]().
