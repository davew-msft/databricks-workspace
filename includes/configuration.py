# Databricks notebook source
# MAGIC %md
# MAGIC Put your vars here

# COMMAND ----------

# for these labs, in a shared workspace we need something like this
username = 'davew'

# COMMAND ----------

mntPath = f"/mnt/lake/"
pipelinePath = f"{username}/"

chkptRoot = f"/checkpoints/"  #it's best if checkpoints are NOT on ADLS2

rawPath = pipelinePath + "raw/"
bronzePath = pipelinePath + "bronze/"
silverPath = pipelinePath + "silver/"
goldPath = pipelinePath + "gold/"


bronzeCheckpoint = chkptRoot + "bronze/"
silverCheckpoint = chkptRoot + "silver/"
goldCheckpoint = chkptRoot + "gold/"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS workshop_{username}")
spark.sql(f"USE workshop_{username}")
