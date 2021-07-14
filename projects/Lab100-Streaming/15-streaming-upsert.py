# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Upserts with Delta Lake
# MAGIC 
# MAGIC Delta Lake ACID compliance enables many operations to take place in the data lake that would normally require a data warehouse. Delta Lake provides `MERGE` syntax to complete updates, deletes, and inserts as a single transaction.
# MAGIC 
# MAGIC Let's do merge logic on a streaming pipeline.   
# MAGIC 
# MAGIC This is especially cool when you need to do SCD2-style processing to a given table in the lake.  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Our Scenario
# MAGIC 
# MAGIC We have some patient data we need to manage via streaming updates.  This isn't SCD2-style, but it gives you the start of a really good pattern.  
# MAGIC 
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC 
# MAGIC The type of action a row represents is not captured, and each user may have many records present. The field `mrn` serves as the unique identifier.  This is the standard pattern that you'll encounter when dealing with streaming data.  The source system "streams" an event that has full-fidelity.  ie, the full record is sent.  But we don't really know (or care) if it is an insert or update...we want to handle that ourselves.  
# MAGIC 
# MAGIC ### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.  Again, this is close to the pattern for SCD2-style processing.  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC 
# MAGIC The following code defines some paths, a demo database, and clears out previous runs of the demo. A helper class is also loaded to the variable `Bronze` to allow us to trigger new data arriving in our `bronze` table.

# COMMAND ----------

# MAGIC %run ./includes/workshop-setup $mode="reset"

# COMMAND ----------

# MAGIC %fs ls /mnt/mtc-workshop/streaming-source

# COMMAND ----------

# MAGIC %run "./includes/upsert-setup"

# COMMAND ----------

# so we can see what we are doing...
# we have 2 folders 
# cdc_raw is where the streaming data will land
# silver is where we want to apply the UPSERT logic
display(dbutils.fs.ls(userhome))
display(dbutils.fs.ls(userhome + "/cdc_raw"))
display(dbutils.fs.ls(userhome + "/silver"))
# note that nothing is in bronzePath yet
# display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# both folders appear to have some data, let's query it and see what's there

# filepath is also mapped to the external table cdc_raw
display(spark.sql(f"SELECT * FROM delta.`{filepath}`"))
display(spark.sql(f"SELECT * FROM cdc_raw"))
display(spark.sql(f"SELECT count(*) FROM cdc_raw"))

# again, nothing here
#display(spark.sql(f"SELECT * FROM delta.`{bronzePath}`"))

# silverPath is also mapped to the external table silver
display(spark.sql(f"SELECT * FROM delta.`{silverPath}`"))
display(spark.sql(f"SELECT * FROM silver"))
display(spark.sql(f"SELECT count(*) FROM silver"))

# take a look at the data and get familiar with it.  


# COMMAND ----------

# MAGIC %md
# MAGIC So the workflow is:  
# MAGIC * data streams from cdc_raw to bronze to silver.  
# MAGIC * think of cdc_raw as a data generator that is streaming new data to bronze
# MAGIC * we will trigger the cdc_raw to bronze with a little utility function called `arrival`
# MAGIC 
# MAGIC Note that silver already has some data that we can use for upserts from bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Bronze Data
# MAGIC Land a batch of data and display our table.

# COMMAND ----------

Bronze.arrival()
display(spark.sql(f"SELECT * FROM delta.`{bronzePath}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using `foreachBatch` with Structured Streaming
# MAGIC 
# MAGIC The Spark Structured Streaming `foreachBatch` method allows you to define custom logic when writing to a sink (which will likely be a `delta` table).
# MAGIC 
# MAGIC The logic applied during `foreachBatch` addresses the present microbatch as if it were a batch (rather than streaming) data. This means that no checkpoint is required for these streams, and that these streams are not stateful.
# MAGIC 
# MAGIC Delta Lake `merge` logic does not have a native writer in Spark Structured Streaming, so this logic must be implemented by applying a custom function within `foreachBatch`.
# MAGIC 
# MAGIC All functions follow the same basic format:
# MAGIC 
# MAGIC ```python
# MAGIC def exampleFunction(microBatchDF, batchID):
# MAGIC     microBatchDF #<do something>
# MAGIC ```
# MAGIC 
# MAGIC The `microBatchDF` argument will be used to capture and manipulate the current microbatch of data as a Spark DataFrame. The `batchID` identifies the microbatch, but can be ignored by your custom writer (but is a necessary argument for `foreachBatch` to work).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Streaming Upsert with Spark SQL
# MAGIC 
# MAGIC The cell below demonstrates using Spark SQL to define a function suitable for performing a streaming upsert. A few things to note:
# MAGIC *  We can only merge into Delta tables (`silver` is an existing Delta table)
# MAGIC *  `createOrReplaceTempView` allows us to create a local view to refer to our microbatch data 
# MAGIC   * using the convention of `stream_batch` is a good pattern and `stream_batch` will be the source in the `USING` clause
# MAGIC * I like to use the pattern below for the `WHEN MATCHED` clause.  This solves issues where I may have to replay a stream or I get duplicate events.  I don't want `non-updating updates` to cause a lot of needless SCD-2 updates.  
# MAGIC 
# MAGIC The code below will update all values if a record with the same `mrn` exists and any values have changed, or insert all values if the record has not been seen. 
# MAGIC 
# MAGIC Records with no changes will be silently ignored.  I call this the `non-updating update`.  It is common in streaming architectures and is safe to ignore

# COMMAND ----------

def upsertToDeltaSQL(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("stream_batch")
    # this is the general pattern to reference a sparkSession in a UDF
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver t
        USING stream_batch s
        ON s.mrn = t.mrn
        WHEN MATCHED AND
          s.dob <> t.dob OR
          s.sex <> t.sex OR
          s.gender <> t.gender OR
          s.first_name <> t.first_name OR
          s.last_name <> t.last_name OR
          s.street_address <> t.street_address OR
          s.zip <> t.zip OR
          s.city <> t.city OR
          s.state <> t.state OR
          s.updated <> t.updated
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Define a stream reading against the bronze Delta table.

# COMMAND ----------

bronzeDF = (spark.readStream
    .format("delta")
    .load(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Use `foreachBatch` to apply our method to each microbatch in our structured stream.
# MAGIC 
# MAGIC We can also do any other ETL to bronzeDF here.  This could be python, scala, or SQL.  We can even create intermediate DFs...we don't need to do everything to `bronzeDF`.  We just ensure that the final DF is the DF that we apply `writeStream` to.  
# MAGIC 
# MAGIC When you run this cell it will start the stream.

# COMMAND ----------

(bronzeDF.writeStream
    .format("delta")
    .foreachBatch(upsertToDeltaSQL)
    .outputMode("update")
#     .trigger(once=True)
    .trigger(processingTime='2 seconds')
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following query to see the newest updated records first.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Land new data in the `bronze` table and track progress through your streaming query above. Also note the changes to the streaming dashboard

# COMMAND ----------

Bronze.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC Now you should be able to see new data inserted and updated in your table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Stop your stream before continuing.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining a Streaming Upsert with Python
# MAGIC We used SQL above, but can use python or scala.  In either case, `foreachBatch` must be used to apply `merge` logic with Structured Streaming.
# MAGIC 
# MAGIC Note that the logic below is identical to our previous SQL query.
# MAGIC 
# MAGIC As before, our upsert is defined in reference to our target `silver` table. As such, the code below begins by loading the `silver` Delta table using the API.
# MAGIC 
# MAGIC We can see the same conditional logic used to specify which matched records should be updated.

# COMMAND ----------

from delta.tables import *

silver_table = DeltaTable.forPath(spark, silverPath)

def upsertToDelta(microBatchDF, batchId):
    (silver_table.alias("t").merge(
        microBatchDF.alias("s"),
        "s.mrn = t.mrn")
        .whenMatchedUpdateAll(
            condition = """s.dob <> t.dob OR
                            s.sex <> t.sex OR
                            s.gender <> t.gender OR
                            s.first_name <> t.first_name OR
                            s.last_name <> t.last_name OR
                            s.street_address <> t.street_address OR
                            s.zip <> t.zip OR
                            s.city <> t.city OR
                            s.state <> t.state OR
                            s.updated <> t.updated""")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# delta tables have a lot of options, you can see those by using tab completion below
# examples:  clone, delete, history
# silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history 'dbfs:/user/davew@microsoft.com/intro/silver'

# COMMAND ----------

# MAGIC %md
# MAGIC The code below applies our Python `merge` logic with a streaming write.
# MAGIC 
# MAGIC It's the same code as before, all we changed is `upsertToDelta`

# COMMAND ----------

(bronzeDF.writeStream
    .format("delta")
    .foreachBatch(upsertToDelta)
    .outputMode("update")
#     .trigger(once=True)
    .trigger(processingTime='2 seconds')
    .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Land more data in the `bronze` table.

# COMMAND ----------

Bronze.arrival()

# COMMAND ----------

# MAGIC %md
# MAGIC Once your batch has processed, run the cell below to see newly updated rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver
# MAGIC ORDER BY updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC Using `DESCRIBE HISTORY`, we can review the logic applied during our merge statements, as well as check the metrics for number of rows inserted and updated with each write.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure you stop all streams before continuing to the next notebook.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Below is not working yet.  Need to fix the pathing after rerunning the above.**

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Change Data Feed
# MAGIC 
# MAGIC * keeps track of changes to delta tables.
# MAGIC * useful when you need to see what data has changed in a non-append-only delta table (for instance, a snapshot table like you would find in the gold area of the datalake)
# MAGIC   * streams only work on append-only tables
# MAGIC * we can consume a CDF as a batch or a stream
# MAGIC * CDF is, itself, just a delta table (kinda like a "nested" delta table)
# MAGIC 
# MAGIC Typical Use Cases
# MAGIC 
# MAGIC * materialized views
# MAGIC   * up-to-date aggregated views for dashboards without having to reprocess all underlying data
# MAGIC * as the producer for change data to downstream consumers
# MAGIC   * send the CDF to kafka/EH
# MAGIC * audit trail
# MAGIC   * efficient for querying changes over time, including deletes (which don't track in a delta table)
# MAGIC * any time your delta table can include updates or deletes (ie, the delta destination is not an append-only structure)
# MAGIC * any time you have full overwrites of a delta table
# MAGIC 
# MAGIC How do you use it?
# MAGIC 
# MAGIC * enable it with a spark option (see below)
# MAGIC * change the readStream on the source data to have `.option("readChangeData", True)`
# MAGIC 
# MAGIC Let's look at how you can easily propagate changes through a Lakehouse with the help of CDF.  
# MAGIC 
# MAGIC In our example, we'll work with the same demographic records we used above
# MAGIC 
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC 
# MAGIC The type of action a row represents is not captured.
# MAGIC 
# MAGIC ### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.
# MAGIC 
# MAGIC ### Gold Table
# MAGIC For this example, we'll create a simple gold table that captures patients that have a new address.

# COMMAND ----------

# MAGIC %md
# MAGIC Enable CDC using Spark conf setting; will be used on all Delta tables.

# COMMAND ----------

spark.conf.set('spark.databricks.delta.properties.defaults.enableChangeDataFeed',True)

# COMMAND ----------

# this is our cdc source data
filepath

# COMMAND ----------

# remove bronze folder
dbutils.fs.rm(bronzePath,True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Data with Auto Loader
# MAGIC 
# MAGIC Here we'll use Auto Loader to ingest data as it arrives.
# MAGIC 
# MAGIC The logic below is set up to either use trigger once to process all records loaded so far, or to continuously process records as they arrive.
# MAGIC 
# MAGIC We'll turn on continuous processing.

# COMMAND ----------

schema = "mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated timestamp"

print(bronzePath)
print(filepath)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema)
    .load(filepath)
    .writeStream
    .format("delta")
    .outputMode("append")
#     .trigger(once=True)
    .trigger(processingTime='5 seconds')
    .option("checkpointLocation", userhome + "/_bronze_checkpoint")
    .start(bronzePath))
