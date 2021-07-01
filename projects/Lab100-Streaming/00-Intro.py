# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming Concepts
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC The source contains smartphone accelerometer samples from devices and users with the following columns:
# MAGIC 
# MAGIC | Field          | Description |
# MAGIC | ------------- | ----------- |
# MAGIC | Arrival_Time | time data was received |
# MAGIC | Creation_Time | event time |
# MAGIC | Device | type of Model |
# MAGIC | Index | unique identifier of event |
# MAGIC | Model | i.e Nexus4  |
# MAGIC | User | unique user identifier |
# MAGIC | geolocation | city & country |
# MAGIC | gt | transportation mode |
# MAGIC | id | unused null field |
# MAGIC | x | acceleration in x-dir |
# MAGIC | y | acceleration in y-dir |
# MAGIC | z | acceleration in z-dir |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "workshop"

# COMMAND ----------

# MAGIC %run ./includes/workshop-setup $mode="reset"

# COMMAND ----------

display(dbutils.fs.ls(mount_location))

# COMMAND ----------

#let's look at one file
dbutils.fs.head(f"{mount_location}/00.json")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Micro-Batches as a Table
# MAGIC 
# MAGIC For more information, see [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts) (from which several images have been borrowed).
# MAGIC 
# MAGIC Spark Structured Streaming approaches streaming data by modeling it as a series of continuous appends to an unbounded table. While similar to defining **micro-batch** logic, this model allows incremental queries to be defined against streaming sources as if they were static input.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" style="height: 300px"/>
# MAGIC 
# MAGIC ### Basic Concepts
# MAGIC 
# MAGIC - The developer defines an **input table** by configuring a streaming read against a **source**. The syntax provides entry that is nearly analogous to working with static data.
# MAGIC - A **query** is defined against the input table. Both the DataFrames API and Spark SQL can be used to easily define transformations and actions against the input table.
# MAGIC - This logical query on the input table generates the **results table**. The results table contains the incremental state information of the stream.
# MAGIC - The **output** of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Generally, a sink will be a durable system such as files or a pub/sub messaging bus.
# MAGIC - New rows are appended to the input table for each **trigger interval**. These new rows are essentially analogous to micro-batch transactions and will be automatically propagated through the results table to the sink.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" style="height: 300px"/>
# MAGIC 
# MAGIC It's easy to adapt batch logic to streaming data to run data workloads in near real-time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end _exactly-once_ fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.  The data also has to be _idempotent_.  
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC 
# MAGIC * checkpointing to record the offset range of data being processed during each trigger interval.
# MAGIC * the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The `readStream` method returns a `DataStreamReader` used to configure the stream.  Syntactically, it is almost identical as `read`.  So the development lifecycle is to build your data engineering pipelines by thinking in terms of batches against existing data lake folders, then changing a few lines of code and you have a stream.  
# MAGIC 
# MAGIC Configuring a streaming read on a source requires:
# MAGIC * The schema of the data
# MAGIC * The `format` of the source [(file format or named connector)](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/data-sources)
# MAGIC * Configurations specific to the source:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Event Hubs](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Schema
# MAGIC 
# MAGIC Every streaming DataFrame must have a schema. When connecting to pub/sub systems like Kafka and Event Hubs, the schema will be automatically provided by the source.
# MAGIC 
# MAGIC For other streaming sources, the schema must be user-defined. It is not safe to infer schema from files, as the assumption is that the source is growing indefinitely from zero records.
# MAGIC 
# MAGIC This is our schema:

# COMMAND ----------

schema = "Arrival_Time BIGINT, Creation_Time BIGINT, Device STRING, Index BIGINT, Model STRING, User STRING, geolocation STRUCT<city: STRING, country: STRING>, gt STRING, id BIGINT, x DOUBLE, y DOUBLE, z DOUBLE"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Differences between Static and Streaming Reads
# MAGIC 
# MAGIC In the cell below, a static and streaming read are each defined against the same source (files in a directory on a cloud object store). Note that the syntax is identical except that the streaming query uses `readStream` instead of `read`.

# COMMAND ----------

staticDF = (spark
  .read
  .format("json")
  .schema(schema)
  .load(mount_location)
)

streamingDF = (spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1)     # Optional; force processing of only 1 file per trigger. ie, we throttle the stream
  .load(mount_location)
)

# COMMAND ----------

staticDF.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC Just like with static DataFrames, data is not processed and jobs are not triggered until an action is called.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC The method `DataFrame.writeStream` returns a `DataStreamWriter` used to configure the output of the stream.
# MAGIC 
# MAGIC There are a number of required parameters to configure a streaming write:
# MAGIC * The `format` of the **output sink** (see [documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks))
# MAGIC * The location of the **checkpoint directory**
# MAGIC * The **output mode**
# MAGIC * Configurations specific to the output sink, such as:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Event Hubs](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)
# MAGIC   * A <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=foreach#pyspark.sql.streaming.DataStreamWriter.foreach"target="_blank">custom sink</a> via `writeStream.foreach(...)`
# MAGIC 
# MAGIC Once the configuration is completed, trigger the job with a call to `.start()`. When writing to files, use `.start(filePath)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to Azure Blob Storage or ADLS.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. [More details here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes).
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- |
# MAGIC | **Append** | `.outputMode("append")`     | _DEFAULT_ - Only the new rows appended to the Result Table since the last trigger are written to the sink. |
# MAGIC | **Complete** | `.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
# MAGIC | **Update** | `.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1 |
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Not all sinks will support `update` mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between Static and Streaming Writes
# MAGIC 
# MAGIC The following cell demonstrates batch logic to append data from a static read.

# COMMAND ----------

outputPath = userhome + "/static-write"

dbutils.fs.rm(outputPath, True)    # clear this directory, just in case

(staticDF                                
  .write                                               
  .format("delta")                                          
  .mode("append")                                       
  .save(outputPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Note that there are only minor syntax differences when writing a stream instead:
# MAGIC - `writeStream` instead of `write`
# MAGIC - The path for the checkpoint is provided to the option `checkpointLocation`
# MAGIC - `outputMode` instead of `mode` (note that streaming uses `complete` instead of `overwrite` for similar functionality here)
# MAGIC - `start` instead of `save`
# MAGIC 
# MAGIC The following cell demonstrates a streaming write to Delta files.
# MAGIC 
# MAGIC Assigning a variable name when writing to a sink provides programmatic access to a `StreamingQuery` object. This will be discussed below.

# COMMAND ----------

outputPath = userhome + "/stream-write"
checkpointPath = outputPath + "/checkpoint"

dbutils.fs.rm(outputPath, True)    # clear this directory, just in case

# COMMAND ----------

streamingQuery = (streamingDF                                
  .writeStream                                                
  .format("delta")                                          
  .option("checkpointLocation", checkpointPath)               
  .outputMode("append")
#   .queryName("my_stream")        # optional argument to register stream to Spark catalog
  .start(outputPath)                                       
)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now re-run the cell above a second time.  What happens?  Why?  What would happen if we rm -rf the checkpoint folder?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming with Delta Lake
# MAGIC 
# MAGIC In the logic defined above, data is read from JSON files and then saved out in the Delta Lake format. Note that because Delta creates a new version for each transaction, when working with streaming data this will mean that the Delta table creates a new version for each trigger interval in which new data is processed. [More info on streaming with Delta](https://docs.databricks.com/delta/delta-streaming.html#table-streaming-reads-and-writes).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Defining the Trigger Interval
# MAGIC 
# MAGIC When defining a streaming write, the `trigger` method specifies when the system should process the next set of data. The example above uses the default, which is the same as `.trigger(Trigger.ProcessingTime("500 ms"))`.
# MAGIC 
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | _DEFAULT_ - The query will be executed as soon as the system has completed processing the previous query |
# MAGIC | Fixed interval micro-batches           | `.trigger(Trigger.ProcessingTime("2 minutes"))` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
# MAGIC | Continuous w/fixed checkpoint interval | `.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency, <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing" target = "_blank">continuous processing mode</a>. _EXPERIMENTAL_ in 2.3.2 |
# MAGIC 
# MAGIC Note that triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger; some sources allow configuration to limit the size of each micro-batch.
# MAGIC 
# MAGIC :BEST_PRACTICE: Read [this blog post](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html) to learn more about using `Trigger.Once` to simplify CDC with a hybrid streaming/batch design.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Managing & Monitoring Streaming Queries<
# MAGIC 
# MAGIC Databricks notebooks have built-in interative dashboards that allow users to manually monitor streaming performance.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/adbcore/streaming-dashboard.png)
# MAGIC 
# MAGIC To log or monitor streaming metrics to external systems, users should define a `StreamingQueryListener`, as demonstrated [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#reporting-metrics-programmatically-using-asynchronous-apis).
# MAGIC 
# MAGIC The `StreamingQuery` object can be used to [monitor and manage the stream](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#managing-streaming-queries).
# MAGIC 
# MAGIC The `StreamingQuery` object can be captured as the return of a write definition or accessed from within the active streams list, demonstrated here:

# COMMAND ----------

for s in spark.streams.active:         # Iterate over all streams
    print(s.id)                        # Print the stream's id

# COMMAND ----------

# MAGIC %md
# MAGIC The `recentProgress` attribute allows access to metadata about recently processed micro-batches.

# COMMAND ----------

streamingQuery.recentProgress

# COMMAND ----------

# MAGIC %md
# MAGIC The code below stops the `streamingQuery` defined above and introduces `awaitTermination()`
# MAGIC 
# MAGIC `awaitTermination()` will block the current thread
# MAGIC * Until the stream stops or
# MAGIC * Until the specified timeout elapses

# COMMAND ----------

streamingQuery.awaitTermination(5)      # Stream for another 5 seconds while the current thread blocks
streamingQuery.stop()                   # Stop the stream

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## The Display function
# MAGIC 
# MAGIC Within the Databricks notebooks, we can use the `display()` function to render a live plot. This stream is written to memory; **generally speaking this is most useful for debugging purposes**.
# MAGIC 
# MAGIC When you pass a "streaming" `DataFrame` to `display()`:
# MAGIC * A "memory" sink is being used
# MAGIC * The output mode is complete
# MAGIC * *OPTIONAL* - The query name is specified with the `streamName` parameter
# MAGIC * *OPTIONAL* - The trigger is specified with the `trigger` parameter
# MAGIC * *OPTIONAL* - The checkpointing location is specified with the `checkpointLocation`
# MAGIC 
# MAGIC `display(myDF, streamName = "myQuery")`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The previous cell programmatically stopped only active streaming query. In the cell below, `display` will start a new streaming query against the source defined in `streamingDF`.  We are passing `streaming_display` as the name for this newly started stream.

# COMMAND ----------

display(streamingDF, streamName = "streaming_display")

# COMMAND ----------

# MAGIC %md
# MAGIC Using the value passed to `streamName` in the call to `display`, we can programatically access this specific stream:

# COMMAND ----------

for stream in spark.streams.active:   
  if stream.name == "streaming_display":            
    print("Found {} ({})".format(stream.name, stream.id))

# COMMAND ----------

# MAGIC %md
# MAGIC Since the `streamName` gets registered as a temporary table pointing to the memory sink, we can use SQL to query the sink.

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_display WHERE gt = "stand"

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all remaining streams.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#id2" target="_blank">Failed Streaming Query Recovery</a> Best Practices for Recovery.
# MAGIC * <a href="https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html" target="_blank">Continuous Processing Mode</a> Lowest possible latency stream processing. 
