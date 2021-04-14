# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the taxi_db database in Databricks
# MAGIC 
# MAGIC The 00 notebooks should create the objects needed only for a given project or ETL pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

display(spark.catalog.listDatabases())
