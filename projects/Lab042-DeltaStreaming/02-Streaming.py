# Databricks notebook source
# MAGIC %md 
# MAGIC # Part 2
# MAGIC In the previous notebook we set up a simulated IoT streaming session to `bronze/iot/stream_data`.  We are simulating IoT devices that write directly to that folder structure.  Now we are going to use that as the source data as we stream the IoT data through other zones of the data lake.  

# COMMAND ----------

# you will need to manually cancel this query
# all we are doing is "watching" the data as it streams into the bronze area

# change if needed
datalake_root_project_path = "/mnt/lake/"

from pyspark.sql.functions import col
# Read a stream from delta table
streamDF = (spark
  .readStream
  .format('delta')
  .load("{}/bronze/iot/stream_data".format(datalake_root_project_path)))


# COMMAND ----------

display(streamDF)
# note some interesting things:
# it takes a few seconds for the stream to initialize
# if you look carefully the dataset output is actually changing
# the dashboard will show you the throughput 
# almost all of the initial data will be loaded in the first few batches
# the raw data will tell you _where_ you are at wrt the stream processing.  ie, what is the "lag"
# you can restart and rerun this as much as you want.  It will always restart at the beginning of the stream

# COMMAND ----------

# MAGIC %md 
# MAGIC When you are ready to continue make sure you stop the above cell.  Now we want to simulate a real-time streaming ELT pipeline.  We are going to recycle `streamDF` but we are going to do a modicum of transforms before we sink the data to `silver`
# MAGIC 
# MAGIC Our data is fairly clean an requires very little transformations to get it properly formatted for our silver tables. The entire idea of silver tables is to apply the minimum transforms and a little business logic to create readable tables that can be joined and summarized for consumption in gold.
# MAGIC 
# MAGIC Let's do this for both batch and streaming.
# MAGIC 
# MAGIC One of the nice features of Delta is the ability to use it as both a streaming source and a streaming sink.  This means a stream can read a table and process new data as it appears.  This allows you to do real-time processing without the need for EventHubs or Kafka.  

# COMMAND ----------

# do ETL here
# for now, let's just format timestamp into a human-readable format
streamDF1 = (
  streamDF
    .withColumnRenamed("timestamp", "raw_timestamp")
    .withColumn("Timestamp", (col("raw_timestamp")/1000).cast("timestamp"))
)

# COMMAND ----------

# here is where the magic occurs.
# the writestream function will trigger the end-to-end streaming of the data
# we use a checkpointLocation so we can "pick up where we leave off" if we ever need to stop/restart the stream

# best to use a checkpoint location that is DBFS, not ADLS, for this use case, for speed.  
# make sure you have a naming convention and location that makes sense for all of your streaming workloads

datalake_root_project_path = "/mnt/lake/silver/iot/stream_data"


#DISCUSSION POINT:  what is the process when a change needs to be made to a streaming solution?  
(streamDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints{}".format(datalake_root_project_path)) 
  .outputMode("append") # appends data to our table
  .start("{}".format(datalake_root_project_path))) 

# check your destination and verify the files are being created
# cancel the query when you are satisfied everything is working

# COMMAND ----------

dbutils.fs.rm("/checkpoints/iot/silver/stream_data",True)
