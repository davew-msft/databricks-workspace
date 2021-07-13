# Databricks notebook source
sqlContext.setConf("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# COMMAND ----------

# %fs ls /mnt/mtc-workshop/healthcare

# COMMAND ----------

source_dir = "/mnt/mtc-workshop/healthcare"

streamingPath          = userhome + "/source"
bronzePath             = userhome + "/bronze"
recordingsParsedPath   = userhome + "/silver/recordings_parsed"
recordingsEnrichedPath = userhome + "/silver/recordings_enriched"
dailyAvgPath           = userhome + "/gold/dailyAvg"

checkpointPath               = userhome + "/checkpoints"
bronzeCheckpoint             = userhome + "/checkpoints/bronze"
recordingsParsedCheckpoint   = userhome + "/checkpoints/recordings_parsed"
recordingsEnrichedCheckpoint = userhome + "/checkpoints/recordings_enriched"
dailyAvgCheckpoint           = userhome + "/checkpoints/dailyAvgPath"

print(f"""
other vars used in healthcare/lakehouse/streaming lab:
streamingPath: {streamingPath}
bronzePath: {bronzePath}
recordingsParsedPath: {recordingsParsedPath}
recordingsEnrichedPath: {recordingsEnrichedPath}
dailyAvgPath: {dailyAvgPath}
checkpointPath: {checkpointPath}
bronzeCheckpoint: {bronzeCheckpoint}
recordingsParsedCheckpoint: {recordingsParsedCheckpoint}
recordingsEnrichedCheckpoint: {recordingsEnrichedCheckpoint}
dailyAvgCheckpoint: {dailyAvgCheckpoint}
Copy these to notepad for quick reference throughout this lab!!
""")

# COMMAND ----------

class FileArrival:
  def __init__(self):
    self.source = source_dir + "/tracker/streaming/"
    self.userdir = streamingPath + "/"
    self.curr_mo = 1
    
  def arrival(self, continuous=False):
    if self.curr_mo > 12:
      print("Data source exhausted\n")
    elif continuous == True:
      while self.curr_mo <= 12:
        curr_file = f"{self.curr_mo:02}.json"
        dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
        self.curr_mo += 1
    else:
      curr_file = f"{str(self.curr_mo).zfill(2)}.json"
      dbutils.fs.cp(self.source + curr_file, self.userdir + curr_file)
      self.curr_mo += 1
      
NewFile = FileArrival()

