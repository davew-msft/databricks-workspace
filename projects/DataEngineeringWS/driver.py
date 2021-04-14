# Databricks notebook source
# MAGIC %md
# MAGIC ## Driver Notebook
# MAGIC 
# MAGIC This notebook drives the entire workflow for this project.  [For more information](https://docs.databricks.com/notebooks/notebook-workflows.html)

# COMMAND ----------

dbutils.notebook.run("./01-CREATE/01-CREATESQL",0)

# COMMAND ----------

dbutils.notebook.run("./02-ETL/01-ETL-template",0)
dbutils.notebook.run("./03-AUDIT/auditing",0)
dbutils.notebook.run("./04-PUBLISH/publish",0)
dbutils.notebook.run("./06-POST-PROCESS/post-processing",0)
