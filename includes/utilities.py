# Databricks notebook source
# MAGIC %md
# MAGIC General utility functions go here

# COMMAND ----------

class utils:
  def analyzeTables(databaseAndTable):
    try:
      print("Table: " + databaseAndTable)
      print("....refresh table")
      sql("REFRESH TABLE " + databaseAndTable)
      print("....analyze table")
      sql("ANALYZE TABLE " + databaseAndTable + " COMPUTE STATISTICS")
      print("....done")
    except Exception as e:
      return e 
