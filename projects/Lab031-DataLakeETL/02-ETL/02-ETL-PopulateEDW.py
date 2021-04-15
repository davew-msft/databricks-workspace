# Databricks notebook source
# MAGIC %md
# MAGIC # Populate EDW
# MAGIC 
# MAGIC In this notebook we will look at some patterns to populate an EDW after your main pipeline has completed.  
# MAGIC 
# MAGIC **This lab will require you to set up a SQL Server.**  We don't want to spend time doing that here so this notebook is for reference only.  

# COMMAND ----------

# this is the data we want to send to our EDW
# this is very simple but we can change this query obviously to anything
reportDF = sql("""
select 
  taxi_type,trip_year,count(*) as trip_count
from 
  taxi_db.taxi_trips_mat_view
group by taxi_type,trip_year
""").cache()

# COMMAND ----------

reportDF.show()

# COMMAND ----------

#Database credentials & details - for use with Spark scala for writing
# this cell should probably be part of an "include"
# we assume SQL Server but I have connection string examples for other common data stores in repo


#Secrets
jdbcUsername = dbutils.secrets.get(scope = "gws-sql-db", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "gws-sql-db", key = "password")

#JDBC driver class & connection properties
# Replace the hostname and database name
driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcHostname = "gws-server.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "gws_sql_db"

#JDBC URI
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

#Properties() object to hold the parameters
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : driverClass
}

# COMMAND ----------

# again, this should be an "include"
# here we are calling the SQL Server to get some metadata that we can use to determine what data needs to
# be loaded, if desired.  

def generateBatchID():
  try:
    batchId = 0
    pushdown_query = "(select count(*) as record_count from BATCH_JOB_HISTORY) table_record_count"
    df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
    recordCount = df.first()[0]

    if(recordCount == 0):
      batchId=1
    else:
      pushdown_query = "(select max(batch_id) as current_batch_id from BATCH_JOB_HISTORY) current_batch_id"
      df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
      batchId = int(df.first()[0]) + 1
    return batchId
  except Exception as e:
    return e  
  
def insertBatchMetadata(batch_step_id,batch_step_description,batch_step_status):
	try:
		batch_id = generateBatchID()
		batch_step_time = str(datetime.now())
		queryString ='select "{}" as batch_id,"{}" as batch_step_id,"{}" as batch_step_description,"{}" as batch_step_status,"{}" as batch_step_time'.format(batch_id,batch_step_id,batch_step_description,batch_step_status,batch_step_time)
		insertQueryDF = sqlContext.sql(queryString)
		insertQueryDF.coalesce(1).write.jdbc(jdbcUrl, "batch_job_history", mode="append", properties=connectionProperties)
	except Exception as e:
		return e

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

#Persist data to destination RDBMS
reportDF.coalesce(1).write.jdbc(jdbcUrl, "TRIPS_BY_YEAR", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

# let's do another example
reportDF = sql("""
select 
  taxi_type,trip_year,pickup_hour,count(*) as trip_count
from 
  taxi_db.taxi_trips_mat_view
group by 
  taxi_type,trip_year,pickup_hour
""")

# COMMAND ----------

reportDF.show()

# COMMAND ----------

reportDF.coalesce(1).write.jdbc(jdbcUrl, "TRIPS_BY_HOUR", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

dbutils.notebook.exit("Pass")
