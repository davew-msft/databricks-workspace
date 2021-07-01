# Databricks notebook source
# MAGIC %md
# MAGIC General streaming helper functions 
# MAGIC     

# COMMAND ----------

# we need this here if running this notebook singleton, otherwise, in a normal includes 
# composable architecture the includes/imports notebook would create these
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Python Type Hints
# MAGIC 
# MAGIC We do this below and some aren't familiar with this syntax.  This helps with static code analysis and autocomplete.  And it just makes the intent clear.  
# MAGIC 
# MAGIC In almost every case we are taking in a df, manipulating it, and sending the df back out.  This helps separate the business logic from the implementation details.  
# MAGIC 
# MAGIC An example:  the function below takes and returns a string and is annotated as follows:
# MAGIC 
# MAGIC ```
# MAGIC def greeting(name: str) -> str:
# MAGIC     return 'Hello ' + name
# MAGIC ```
# MAGIC In the function `greeting`, the argument `name` is expected to be of type `str`
# MAGIC and the return type `str`.

# COMMAND ----------

class streamOps:
  def readStreamRaw (spark: SparkSession, rawPath: str) -> DataFrame:
    kafka_schema = "value STRING"
    return spark.readStream.format("text").schema(kafka_schema).load(rawPath)
  
  def createStreamWriter(
    dataframe: DataFrame,
    checkpoint: str,
    name: str,
    partition_column: str,
    mode: str = "append",
    mergeSchema: bool = False,
    ) -> DataStreamWriter:

    stream_writer = (
        dataframe.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .partitionBy(partition_column)
        .queryName(name)
    )

    if mergeSchema:
        stream_writer = stream_writer.option("mergeSchema", True)
    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)
    return stream_writer
  
  def readStreamDelta(spark: SparkSession, deltaPath: str) -> DataFrame:
    return spark.readStream.format("delta").load(deltaPath)
  
  def stopAllStreams() -> bool:
    stopped = False
    for stream in spark.streams.active:
        stopped = True
        stream.stop()
    return stopped
  
  def stopNamedStream(spark: SparkSession, namedStream: str) -> bool:
    stopped = False
    for stream in spark.streams.active:
        if stream.name == namedStream:
            stopped = True
            stream.stop()
    return stopped
  
  def untilStreamIsReady(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True
  

# COMMAND ----------

# these are just examples, this code does nothing but give you a feel for how to structure 
# spark code as you take your Databricks journey

# what is this doing?  
# I call `useCaseETL.transformBronzeToSilver(dfSource)` or similar
# this function will 
class useCaseETL:
  def transformBronzeToSilver(bronze: DataFrame) -> DataFrame:
    
    # what is this doing?  
    # I call `useCaseETL.transformBronzeToSilver(dfSource)` or similar
    # this function will declare my schema, convert a raw JSON string into the schema, 
    # then do whatever ETL I need it to do like formatting datetimes
    # then it returns the dataframe back to the caller with all of the ETL done.  
    # in theory this should make the main notebook easier to read

    json_schema = "device_id INTEGER, heartrate DOUBLE, device_type STRING, name STRING, time FLOAT"

    return (
        bronze.select(from_json(col("value"), json_schema).alias("nested_json"))
        .select("nested_json.*")
        .select(
            "device_id",
            "device_type",
            "heartrate",
            from_unixtime("time").cast("timestamp").alias("eventtime"),
            "name",
            from_unixtime("time").cast("date").alias("p_eventdate"),
        )
    )
  
  
  def transformRawToBronzeAuditing(raw: DataFrame) -> DataFrame:
    # this is my standard pattern to take a source dataset and add some auditing cols
    return raw.select(
        lit("/mnt/lake/something").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )
  
  def transformSilverAggs(silver: DataFrame) -> DataFrame:
    return silver.groupBy("device_id").agg(
        mean(col("heartrate")).alias("mean_heartrate"),
        stddev(col("heartrate")).alias("std_heartrate"),
        max(col("heartrate")).alias("max_heartrate"),
    )


