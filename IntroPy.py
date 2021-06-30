# Databricks notebook source
# MAGIC %md
# MAGIC ## Getting to Know Databricks Notebooks
# MAGIC 
# MAGIC This is a "documentation cell".  Double-click it to edit it.  
# MAGIC 
# MAGIC The `%md` is known as a "cell magic".  A cell magic alters the behavior of a cell.  
# MAGIC 
# MAGIC Click the keyboard icon above to see notebook keyboard shortcuts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks have some Apache Spark variables already defined
# MAGIC 
# MAGIC Why is this important to know?  Often you'll find Spark documentation that will declare or reference this variables, it's helpful to know how Databricks is doing this for you.  
# MAGIC 
# MAGIC `SparkContext` is `sc`  
# MAGIC `SQLContext` and `HiveContext` is `sqlContext`  
# MAGIC `SparkSession` is `spark`

# COMMAND ----------

# to run a cell simply click it and press Shift+Enter
# This is a python notebook, so unless you have a cell magic that alters the state of the cell you must write python.  Notebooks can 
# be created with various default interpreters.  

# this command will run and show the SparkContext
sc

# COMMAND ----------

# MAGIC %md
# MAGIC The `Out[1]:` entry shows any output from executing a cell.

# COMMAND ----------

# MAGIC %sh 
# MAGIC # this cell uses a bash cell magic and will allow you to run shell code
# MAGIC # the Databricks "user" is ubuntu.  We can see any data we may have uploaded for our acct
# MAGIC # note the symlinks
# MAGIC ls -alF /home/ubuntu/databricks

# COMMAND ----------

# MAGIC %md
# MAGIC However, files we upload to that location will not be distributed to all worker nodes, only the "driver" node.  
# MAGIC the correct way is to look at the "databricks filesystem", which is available to all worker nodes.  Note the different cell magic for the next cell.  %fs is meant to be equivalent to hdfs commands but for the DBFS.  
# MAGIC 
# MAGIC 
# MAGIC Databricks ships with tutorial datasets.

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %md 
# MAGIC The general "convention" is that anything you create should go into `/mnt`.  
# MAGIC 
# MAGIC It's helpful to understand how mounts work across databricks workspaces and clusters.  

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# we can also run Databricks commands using python and not a cell magic.  We simply use the databricks python classes.  Here is one example
display(dbutils.fs.ls("dbfs:/databricks-datasets/samples/"))
# display is a function that allows the results to be in notebook table format which makes copy/paste and downloading results easier.  

# COMMAND ----------

# let's read in a sample file to an RDD (it's actually a pyspark dataframe since this is a python cell) and "infer" its schema
# the trailing \ makes the command easier to read
# Note that nothing will really happen.  If you are quick you can view the DAG (how Spark is going to execute the request)

# this is a demo and thus a small file.  In the big data world you would simply set filepath to the directory of MANY datafiles and Spark will distribute the 
# workload to all worker nodes in the cluster.  

filepath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
df_diamonds = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(filepath)

# COMMAND ----------

# we inferred the schema, but what does this dataset really look like? 
df_diamonds.printSchema()

# COMMAND ----------

type(df_diamonds)  # this confirms that we have created a dataframe (note we can also use this from SparkSQL)
display(df_diamonds)  # this is similar to "select * from table LIMIT 1000".  In general it's better to use "head" or "tail" command for speed, but display 
# has benefits noted above, plus I can graph the data.  
# Try it.  Aggregation=AVG, Keys=cut, Groupings=color, values=price


# COMMAND ----------

# basic data exploration can be done with the describe command which is the pyspark equivalent of R's summary function
# we always wrap in "display" so we can see the output in a pretty HTML table
display(df_diamonds.describe())

# COMMAND ----------

# Here we use python to do some grouping.  Note the syntax is VERY similar to SQL or pandas.  
# We also create 2 additional dataframes.  This does NOT mean that we've duplicated the data.  
# RDDs (and thus dataframes) are only pointers to the original data plus the metadata needed to generate the data when needed.  
# transformations vs actions

df_diamonds_grouped = df_diamonds.groupBy("cut", "color").avg("price") 

# we are joining our "grouped df" to the original df where we are building an on-the-fly aggregation
df_diamonds_final = df_diamonds_grouped\
  .join(df_diamonds, on='color', how='inner')\
  .select("`avg(price)`", "carat")


# COMMAND ----------

# MAGIC %md
# MAGIC Note: that was almost instantaneous.  That's because no "action" was done, only a "transformation".  Spark only carries out actions and instead waits for you to code up all needed transformations first.  This optimization is a key to Spark being so performant, especially over huge datasets.  
# MAGIC 
# MAGIC An action is anything that requires the transformations to be done.  During development you often want to see if your transformation code actually works.  The easiest way to do that is with a `take` action.  This will take a few seconds to execute.

# COMMAND ----------

df_diamonds_final.take(10)
# display(df_diamonds_final.take(10)) #notice the difference

# COMMAND ----------

# this is a tiny dataset but with realworld data you'll find that Spark and Databricks can be slow when accessing csv and json raw data.  
# caching the df is one trick, but it's often better to save the data in parquet format, which is a columnar format optimized for Spark.  This 
# is a really good trick if you are going to be running many queries over the same df or raw data files.  

# tab completion *should* work for this (filed a bug that this doesn't always work)
(df_diamonds
   .write
   .mode("overwrite")
   .format("parquet")
   .saveAsTable("diamonds_pqt")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds_pqt

# COMMAND ----------

# MAGIC %md
# MAGIC Notebooks are known as REPL environments (read, evaluate, print, loop).  Not every programming language supports REPL environments, but all Spark-supported languages do.  With a REPL we place small pieces of code with explanations of our thought processes.  But, if we find we made a mistake higher up in the notebook we can change a cell and choose the `Run All Below` option.  This let's you change your code and quickly re-execute only what is needed.
# MAGIC 
# MAGIC Try it:  Let's assume we want the Sum instead of avg and the `carat` to be displayed as the first column
# MAGIC 
# MAGIC * change avg(price) TWICE to sum(price)
# MAGIC * change `.select("carat","`sum(price)`")`

# COMMAND ----------

# MAGIC %md
# MAGIC Unless you know Python it can be very tedious.  The above demo might be easier for most people using SparkSQL.  Let's try that.  
# MAGIC 
# MAGIC Remember that `df_diamonds` was our original dataframe from data we read from a csv.  Let's convert that to a SQL table.  
# MAGIC Let's use autocompletion.  Type `df_diamonds.creat` and then `tab`.  Note, this works inconsistently in Azure Databricks.  This is a known issue.

# COMMAND ----------

df_diamonds.createOrReplaceTempView("diamonds")

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM diamonds LIMIT 10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS AvgPriceByCarat;
# MAGIC 
# MAGIC CREATE TABLE AvgPriceByCarat AS
# MAGIC SELECT 
# MAGIC   avg(price) AS AvgPrice,
# MAGIC   carat
# MAGIC FROM diamonds
# MAGIC GROUP BY carat;
# MAGIC 
# MAGIC SELECT * FRoM AvgPriceByCarat
# MAGIC /* we don't need the CTAS above unless we may want to use that data in the future */

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why not just use SparkSQL all the time?
# MAGIC 
# MAGIC Wouldn't it be better for a SQL expert to just use SQL?  
# MAGIC 
# MAGIC Maybe use SQL notebooks instead of python as the default?  

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 
# MAGIC In the cells above I first "referenced" the data in a pySpark df and then "mixed-and-matched" that with SparkSQL.  
# MAGIC 
# MAGIC But you CAN have a pure SparkSQL solution without an intermediate pySpark df
# MAGIC 
# MAGIC One problem with this is that you can no longer "infer" the schema.  
# MAGIC */
# MAGIC 
# MAGIC DROP TABLE IF EXISTS diamonds_direct;
# MAGIC 
# MAGIC CREATE TABLE diamonds_direct (
# MAGIC   id int,
# MAGIC   carat double,
# MAGIC   cut string,
# MAGIC   color string,
# MAGIC   clarity string,
# MAGIC   depth double,
# MAGIC   tbl double,
# MAGIC   price integer,
# MAGIC   x double,
# MAGIC   y double,
# MAGIC   z double
# MAGIC )
# MAGIC using csv options (
# MAGIC   path '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv',
# MAGIC   delimiter ',',
# MAGIC   header 'true'
# MAGIC );
# MAGIC 
# MAGIC SELECT * FRoM diamonds_direct LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC * Notebooks are automatically saved but can be exported so they can be run on other Spark clusters or Databricks instances outside of Azure
# MAGIC * When the notebook is closed the memory is freed (it may be freed sooner if the Spark cluster is auto-terminated), but if you re-open the notebook you will see all of the result cells.  Notebooks are saved with the result cells.  This allows you to share your notebooks with others without them having to re-execute all of your code (and wait).  
# MAGIC   * There is a `Clear` option on the menu bar.  
# MAGIC   * You can also perform a `Run All` to get the latest data at any time.

# COMMAND ----------


