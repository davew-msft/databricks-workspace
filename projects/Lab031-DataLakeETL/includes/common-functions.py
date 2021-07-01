# Databricks notebook source
import os
import math
import glob
import re

# COMMAND ----------

prqShrinkageFactor = 0.19 #We found a saving in space of 81% with Parquet

# COMMAND ----------

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

# COMMAND ----------

def calcOutputFileCountTxtToPrq(srcDataFile, targetedFileSizeMB):
  try:
    estFileCount = int(math.floor((os.path.getsize(srcDataFile) * prqShrinkageFactor) / (targetedFileSizeMB * 1024 * 1024)))
    if(estFileCount == 0):
      return 1 
    else:
      return estFileCount
  except Exception as e:
    return e                     

# COMMAND ----------

#Delete residual files from job operation (_SUCCESS, _start*, _committed*)
#Should be called with '/dbfs/mnt/...'
def recursivelyDeleteSparkJobFlagFiles(directoryPath):
  try:
    files = glob.glob(directoryPath + '/**/*', recursive=True)
    for file in files:
      if not os.path.basename(file).endswith('parquet') and os.path.isfile(file):
        fileReplaced = re.sub('/dbfs', 'dbfs:',file)
        print("Deleting...." +  fileReplaced)
        dbutils.fs.rm(fileReplaced)
  except Exception as e:
    return e 

# COMMAND ----------

# mount and unmount functions
def unmount (path):
  """unmount("/mnt/taxistream")"""
  try:
    dbutils.fs.unmount(path)
  except Exception as e: 
    #print(e)
    print("Directory already unmounted?: %s" % path)

# helper function for mounting blob storage with a sas token
def mount_blob_using_sas(storage_account, container,folder, token): 
  
  confkey = "fs.azure.sas.%s.%s.blob.core.windows.net" % (container,storage_account)
  #print (confkey)
  try:
    dbutils.fs.mount( 
      source = "wasbs://%s@%s.blob.core.windows.net/%s" % (container,storage_account,folder),
      mount_point = '/mnt/' + container, 
      extra_configs = {confkey:token}
    )
  except Exception as e: 
    #print(e)
    print("Directory may already be mounted: %s" % container)

# COMMAND ----------

# streaming helper function
def untilStreamIsReady(name):
  queries = list(filter(lambda query: query.name == name, spark.streams.active))

  if len(queries) == 0:
    print("The stream is not active.")

  else:
    while (queries[0].isActive and len(queries[0].recentProgress) == 0):
      pass # wait until there is any type of progress

    if queries[0].isActive:
      queries[0].awaitTermination(5)
      print("The stream is active and ready.")
    else:
      print("The stream is not active.")

None

# COMMAND ----------

print("Building TaxiData class")
class TaxiData:
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

print("done loading common-functions")
