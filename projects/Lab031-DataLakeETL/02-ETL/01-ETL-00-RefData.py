# Databricks notebook source
# MAGIC %md
# MAGIC ETL the reference data
# MAGIC 
# MAGIC When you are ready to productionize this code as a scheduled pipeline you would remove anything that prints unneeded output.  
# MAGIC 
# MAGIC The actual process would be the DevOps engineer (which could be the same as the developer) would change all of the hard-coded locations and objects and move these to the proper area of the lake.  We are not doing that for these labs.  
# MAGIC 
# MAGIC ## In this lab
# MAGIC 
# MAGIC 1. run the common functions so we can reuse them
# MAGIC 2. create external unmanaged hive tables (ppl understand sql better than spark)
# MAGIC 1. load reference data from our wasb acct to our data lake
# MAGIC 3. create statistics 
# MAGIC 
# MAGIC 
# MAGIC In the next few cells we make sure our folder hierarchy is correct

# COMMAND ----------

#Imports cell, this is generally copy/paste for all notebooks, once you get a format that has everything you need.  This is missing a lot.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType

# COMMAND ----------

# vars to change
srcDataDirRoot = "/mnt/wasb-nyctaxi-staging/reference-data/" #Root dir for source data
destRoot = "/mnt/lake/raw/"
destProjDir = "%snyctaxi/" % destRoot
destRefDir = "%sreference/" %  destProjDir #Root dir for consumable data
print (destRefDir)

# COMMAND ----------

# adjust paths above if needed
display(dbutils.fs.ls("dbfs:%s" % srcDataDirRoot))

# COMMAND ----------

# the lake probably doesn't have these folders, build them
dbutils.fs.mkdirs("dbfs:%s" % (destRefDir))

# COMMAND ----------

# adjust paths above if needed
display(dbutils.fs.ls("dbfs:%s" % destRefDir))
# this will fail until a file exists, that's ok and it's why we can't infer schema

# COMMAND ----------

# MAGIC %fs ls /mnt/lake

# COMMAND ----------

# MAGIC %md
# MAGIC Call another notebook.  You may need to adjust paths.  You can rclick a notebook under the `Workspace` callout menu to copy the relative path

# COMMAND ----------

# MAGIC %run ./includes/common-functions

# COMMAND ----------

# MAGIC %md
# MAGIC Let's list our reference datasets

# COMMAND ----------

display(dbutils.fs.ls(srcDataDirRoot))

# COMMAND ----------

# MAGIC %md
# MAGIC Raw reference data schema

# COMMAND ----------

# 1.  Taxi zone lookup
taxiZoneSchema = StructType([
    StructField("location_id", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("service_zone", StringType(), True)])

#2. Months of the year
tripMonthNameSchema = StructType([
    StructField("trip_month", StringType(), True),
    StructField("month_name_short", StringType(), True),
    StructField("month_name_full", StringType(), True)])

#3.  Rate code id lookup
rateCodeSchema = StructType([
    StructField("rate_code_id", IntegerType(), True),
    StructField("description", StringType(), True)])

#4.  Payment type lookup
paymentTypeSchema = StructType([
    StructField("payment_type", IntegerType(), True),
    StructField("abbreviation", StringType(), True),
    StructField("description", StringType(), True)])

#5. Trip type
tripTypeSchema = StructType([
    StructField("trip_type", IntegerType(), True),
    StructField("description", StringType(), True)])

#6. Vendor ID
vendorSchema = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("abbreviation", StringType(), True),
    StructField("description", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Reference data
# MAGIC 
# MAGIC There are many ways to load data.  This actually isn't my favorite, but it's the fastest way to just copy files when you want to use dbx.  This could also go in a common functions library.

# COMMAND ----------

def loadReferenceData(srcDatasetName, srcDataFile, destDataDir, srcSchema, delimiter ):
  print("Dataset:  " + srcDatasetName)
  print(".......................................................")
  
  #Execute for idempotent runs
  print("....deleting destination directory - " + str(dbutils.fs.rm(destDataDir, recurse=True)))
  
  #Read source data
  refDF = (sqlContext.read.option("header", True)
                      .schema(srcSchema)
                      .option("delimiter",delimiter)
                      .csv(srcDataFile))
      
  #Write parquet output
  print("....reading source and saving as parquet")
  refDF.coalesce(1).write.parquet(destDataDir)
  
  #Delete residual files from job operation (_SUCCESS, _start*, _committed*)
  #print "....deleting flag files"
  #dbutils.fs.ls(destDataDir + "/").foreach(lambda i: if (!(i.path contains "parquet")) dbutils.fs.rm(i.path))
  
  print("....done")

# COMMAND ----------

# MAGIC %md
# MAGIC * before executing these cells, consider looking at the data files usuing storage explorer so you understand the format.   
# MAGIC * also, consider running only one load at a time
# MAGIC * check your lake to ensure the data landed correctly
# MAGIC * yes, we are moving directly to parquet and skipping landing.  This is reference data and likely shouldn't change much.  However, if you want to stick to a reusable pattern, I just violated that best practice

# COMMAND ----------

display(dbutils.fs.ls(destRefDir))

# COMMAND ----------

## Load the data
loadReferenceData("taxi zone",srcDataDirRoot + "taxi_zone_lookup.csv",destRefDir + "taxi-zone",taxiZoneSchema,",")
loadReferenceData("trip month",srcDataDirRoot + "trip_month_lookup.csv",destRefDir + "trip-month",tripMonthNameSchema,",")
loadReferenceData("rate code",srcDataDirRoot + "rate_code_lookup.csv",destRefDir + "rate-code",rateCodeSchema,"|")
loadReferenceData("payment type",srcDataDirRoot + "payment_type_lookup.csv",destRefDir + "payment-type",paymentTypeSchema,"|")
loadReferenceData("trip type",srcDataDirRoot + "trip_type_lookup.csv",destRefDir + "trip-type",tripTypeSchema,"|")
loadReferenceData("vendor",srcDataDirRoot + "vendor_lookup.csv",destRefDir + "vendor",vendorSchema,"|")

# COMMAND ----------

display(dbutils.fs.ls(destRefDir))

# COMMAND ----------

# MAGIC %md
# MAGIC create SQL objects.  Note that these could now be put into the sql objects file too.

# COMMAND ----------

# MAGIC %sql 
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS taxi_zone_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS taxi_zone_lookup(
# MAGIC location_id STRING,
# MAGIC borough STRING,
# MAGIC zone STRING,
# MAGIC service_zone STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/taxi-zone/';
# MAGIC 
# MAGIC ANALYZE TABLE taxi_zone_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.taxi_zone_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS trip_month_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS trip_month_lookup(
# MAGIC trip_month STRING,
# MAGIC month_name_short STRING,
# MAGIC month_name_full STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/trip-month/';
# MAGIC 
# MAGIC ANALYZE TABLE trip_month_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.trip_month_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS rate_code_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS rate_code_lookup(
# MAGIC rate_code_id INT,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/rate-code/';
# MAGIC 
# MAGIC ANALYZE TABLE rate_code_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.rate_code_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS payment_type_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS payment_type_lookup(
# MAGIC payment_type INT,
# MAGIC abbreviation STRING,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/payment-type/';
# MAGIC 
# MAGIC ANALYZE TABLE payment_type_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.payment_type_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS trip_type_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS trip_type_lookup(
# MAGIC trip_type INT,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/trip-type/';
# MAGIC 
# MAGIC ANALYZE TABLE trip_type_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.trip_type_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxi_db;
# MAGIC DROP TABLE IF EXISTS vendor_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS vendor_lookup(
# MAGIC vendor_id INT,
# MAGIC abbreviation STRING,
# MAGIC description STRING)
# MAGIC USING parquet
# MAGIC LOCATION '/mnt/lake/raw/nyctaxi/reference/vendor/';
# MAGIC 
# MAGIC ANALYZE TABLE vendor_lookup COMPUTE STATISTICS;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxi_db.vendor_lookup;

# COMMAND ----------


