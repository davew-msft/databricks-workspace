# Databricks notebook source
# MAGIC %md
# MAGIC We want to mount our blob and ADLSgen2 here.  We want this notebook to be callable if we setup any new clusters/workspaces.  
# MAGIC 
# MAGIC We do NOT want it to unnecessarily print out a bunch of debugging and status messages, so make sure you remove them once everything is tested.  
# MAGIC 
# MAGIC As always, run every notebook at least TWICE to be sure it is idempotent.
# MAGIC 
# MAGIC You will need to create your SAS token from storage explorer or from az cli.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Tangent:  Another Way to Mount Storage
# MAGIC 
# MAGIC You can mount storage directly to a cluster by setting up the necessary spark configs directly on the cluster on the Clusters page of your workspace, in the `Spark Configs` box.  You can then restart your cluster.  
# MAGIC 
# MAGIC I don't like this approach because it is bound to a single cluster.  Any new clusters that we create we need to remember to add these configs.  I think it's much better to programmatically _bootstrap_ the mounts using idempotent notebooks.  
# MAGIC 
# MAGIC I'm interested in your opinion, do you agree with me or do you think these settings are better implemented on a cluster-by-cluster basis in the spark configs?

# COMMAND ----------

# MAGIC %py
# MAGIC # this is a one-time operation to mount our WASB container using MY WASB container.  Adjust accordingly
# MAGIC # generally this would be run in a notebook all by itself.  
# MAGIC # mounts survive a Databricks shutdown/restart
# MAGIC 
# MAGIC CONTAINER = "nyctaxi-staging"
# MAGIC ACCTNAME = "davewdemoblobs"
# MAGIC FOLDER = ""
# MAGIC TOKEN = "?sp=rl&st=2021-04-06T16:56:33Z&se=2030-12-02T01:56:33Z&spr=https&sv=2020-02-10&sr=c&sig=3M6B4oVevEEPPoB2rzkUcO4x0YFIKe67SWAkGOX3BdQ%3D"
# MAGIC CONFKEY = "fs.azure.sas.%s.%s.blob.core.windows.net" % (CONTAINER,ACCTNAME)
# MAGIC MOUNTDIR = "/mnt/wasb-nyctaxi-staging"
# MAGIC #URI = "https://davewdataengineeringblob.blob.core.windows.net/nyctaxi-staging?st=2019-02-17T15%3A18%3A00Z&se=2032-02-18T..."
# MAGIC 
# MAGIC #dbutils.fs.mkdirs("dbfs:%s" % (MOUNTDIR))
# MAGIC #print(CONFKEY)
# MAGIC 
# MAGIC try: 
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://%s@%s.blob.core.windows.net/%s" % (CONTAINER,ACCTNAME,FOLDER),
# MAGIC     mount_point = MOUNTDIR,
# MAGIC     extra_configs = {CONFKEY: TOKEN})
# MAGIC except Exception as e: 
# MAGIC   print(e)
# MAGIC   print("Directory already mounted?")
# MAGIC   
# MAGIC   

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %fs ls /mnt/wasb-nyctaxi-staging

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now mount ADLSgen2  
# MAGIC 
# MAGIC There are various ways to mount ADLSgen2 and there are various reasons why.  I think the easiest method, that covers the most use cases, is [using a Service Principal](https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html).  You should've set this up already in a previous lab.  
# MAGIC 
# MAGIC Note we use the `ABFSS` driver below.  ABFS stands for Azure Blob FileSystem Driver to connect and retrieve the data from the Azure Data Lake Storage Gen2.  
# MAGIC 
# MAGIC Fill in the variables as needed below.
# MAGIC 
# MAGIC [Documentation](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough) if you want to use credential pass-through (there are limitations)

# COMMAND ----------


directoryID = "72f988bf-86f1-41af-91ab-..."
applicationID = "af2100fe-44ec-419f-a594-..."
keyValue = "HdT1a2v..."
storageAccountName = "davewdemodata"
fileSystemName = "lake"
mntLocation = "lake"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationID,
           "fs.azure.account.oauth2.client.secret": keyValue,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)}

try:
  dbutils.fs.mount(
    source = "abfss://{}@{}.dfs.core.windows.net/".format(fileSystemName, storageAccountName),
    mount_point = "/mnt/{}".format(fileSystemName),
    extra_configs = configs)
except Exception as e: 
  #print(e)
  print("Directory already mounted?")
  


# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/lake"))

# COMMAND ----------

# MAGIC %fs ls /mnt/lake

# COMMAND ----------

# if needed
#dbutils.fs.unmount("/mnt/wasb-nyctaxi-staging")
#dbutils.fs.unmount("/mnt/lake")
