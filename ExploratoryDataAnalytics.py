# Databricks notebook source
# MAGIC %md ## How I do Data Discovery and EDA (Exploratory Data Analytics)
# MAGIC 
# MAGIC I'll walk you through how I "learn" a new dataset.  We'll use a sample "flights" dataset.  We use notebook to get our data to "tell a story" and help the next analyst understand what we've done so far.  
# MAGIC 
# MAGIC We want to get to a point where we know enough about the data that we can build a rudimentary ML model against the existing data to predict if a flight will be delayed or not.  

# COMMAND ----------

# vars to change (TODO)
# mount Dave's storage using a sas token, you can use my storage so you shouldn't have to change these vars

storage_account = "davewdemoblobs"
container = "flights"
folder = ""
token = "?sv=2020-04-08&st=2020-04-16T20%3A14%3A00Z&se=2026-04-17T20%3A14%3A00Z&sr=c&sp=rl&sig=MKQAEXws3iNLxGrzIbzKDom%2FR9Liz%2FgExTFARQHtImo%3D"

# COMMAND ----------

# copied from common-functions.ipynb
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

# MAGIC %md doublecheck everything mounted correctly

# COMMAND ----------

mount_blob_using_sas(storage_account, container,folder, token)

# COMMAND ----------

# MAGIC %fs ls /mnt/flights

# COMMAND ----------

flights_csv = '/mnt/flights/raw-flight-data.csv'
airports_csv = '/mnt/flights/airports.csv'

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

flightSchema = StructType([
  StructField("DayofMonth", IntegerType(), False),
  StructField("DayOfWeek", IntegerType(), False),
  StructField("Carrier", StringType(), False),
  StructField("OriginAirportID", IntegerType(), False),
  StructField("DestAirportID", IntegerType(), False),
  StructField("DepDelay", IntegerType(), False),
  StructField("ArrDelay", IntegerType(), False),
])

flights = spark.read.csv(flights_csv, schema=flightSchema, header=True)

flights.show()

# COMMAND ----------

# frankly, I think display looks better.  display gives a tabular output whenever it can
display(flights)

# COMMAND ----------

# MAGIC %md ### Infer a Data Schema
# MAGIC If the structure of the data source is unknown, you can have Spark automatically infer the schema.
# MAGIC 
# MAGIC In this case, you will load data about airports without knowing the schema.  This is the most common use case...I have a new dataset and I have no idea what it contains

# COMMAND ----------

airports = spark.read.csv(airports_csv, header=True, inferSchema=True)
airports.show()

# COMMAND ----------

print(airports.schema)

# COMMAND ----------

# MAGIC %md ### Use Dataframe Methods
# MAGIC Spark DataFrames provide functions that you can use to extract and manipulate data. For example, you can use the **select** function to return a new dataframe containing columns selected from an existing dataframe.

# COMMAND ----------

cities = airports.select("city", "name")
cities.show()

# COMMAND ----------

# MAGIC %md ### Combine Operations
# MAGIC You can combine functions in a single statement to perform multiple operations on a dataframe. In this case, you will use the **join** function to combine the **flights** and **airports** dataframes, and then use the **groupBy** and **count** functions to return the number of flights from each airport.

# COMMAND ----------

# how would you change this command to sort the output to show the airports with the most originating flights at the top?  
flightsByOrigin = flights.join(airports, flights.OriginAirportID == airports.airport_id).groupBy("city").count()
flightsByOrigin.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC _We could've also done the above in Scala, or more likely, SQL_.
# MAGIC 
# MAGIC ### Count the Rows in a Dataframe
# MAGIC Now that you're familiar with working with dataframes, a key task when building predictive solutions is to explore the data, determing statistics that will help you understand the data before building predictive models. For example, how many rows of flight data do you actually have?

# COMMAND ----------

flights.count()

# COMMAND ----------

# MAGIC %md ### Determine the Presence of Duplicates
# MAGIC The data you have to work with won't always be perfect - often you'll want to *clean* the data; for example to detect and remove duplicates that might affect your model. You can use the **dropDuplicates** function to create a new dataframe with the duplicates removed, enabling you to determine how many rows are duplicates of other rows.

# COMMAND ----------

flights.count() - flights.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md ### Identify Missing Values
# MAGIC As well as determining if duplicates exist in your data, you should detect missing values, and either remove rows containing missing data or replace the missing values with a suitable relacement. The **dropna** function creates a dataframe with any rows containing missing data removed - you can specify a subset of columns, and whether the row should be removed in *any* or *all* values are missing. You can then use this new dataframe to determine how many rows contain missing values.

# COMMAND ----------

flights.count() - flights.dropDuplicates().dropna(how="any", subset=["ArrDelay", "DepDelay"]).count()

# COMMAND ----------

# MAGIC %md ### Clean the Data
# MAGIC Now that you've identified that there are duplicates and missing values, you can clean the data by removing the duplicates and replacing the missing values. The **fillna** function replaces missing values with a specified replacement value. In this case, you'll remove all duplicate rows and replace missing **ArrDelay** and **DepDelay** values with **0**.

# COMMAND ----------

data=flights.dropDuplicates().fillna(value=0, subset=["ArrDelay", "DepDelay"])
data.count()

# COMMAND ----------

# MAGIC %md ## Explore the Data
# MAGIC Now that you've cleaned the data, you can start to explore it and perform some basic analysis. Let's start by examining the lateness of a flight. The dataset includes the **ArrDelay** field, which tells you how many minutes behind schedule a flight arrived. However, if a flight is only a few minutes behind schedule, you might not consider it *late*. Let's make our definition of lateness such that flights that arrive within 25 minutes of their scheduled arrival time are considered on-time, but any flights that are more than 25 minutes behind schedule are classified as *late*. We'll add a column to indicate this classification:

# COMMAND ----------

data = data.select("DayofMonth", "DayOfWeek", "Carrier", "OriginAirportID","DestAirportID",
                   "DepDelay", "ArrDelay", ((col("ArrDelay") > 25).cast("Int").alias("Late")))
display(data)

# COMMAND ----------

# MAGIC %md ### Explore Summary Statistics and Data Distribution
# MAGIC Predictive modeling is based on statistics and probability, so we should take a look at the summary statistics for the columns in our data. The **describe** function returns a dataframe containing the **count**, **mean**, **standard deviation**, **minimum**, and **maximum** values for each numeric column.

# COMMAND ----------

display(data.describe())

# COMMAND ----------

# MAGIC %md 
# MAGIC * The *DayofMonth* is a value between 1 and 31, and the mean is around halfway between these values; which seems about right. 
# MAGIC * The same is true for the *DayofWeek* which is a value between 1 and 7. 
# MAGIC * *Carrier* is a string, so there are no numeric statistics
# MAGIC * we can ignore the statistics for the airport IDs - they're just unique identifiers for the airports, not actually numeric values. 
# MAGIC * The departure and arrival delays range between 63 or 94 minutes ahead of schedule, and over 1,800 minutes behind schedule. The means are much closer to zero than this, and the standard deviation is quite large; so there's quite a bit of variance in the delays. 
# MAGIC * The *Late* indicator is a 1 or a 0, but the mean is very close to 0; which implies that there significantly fewer late flights than non-late flights.
# MAGIC 
# MAGIC Let's verify that assumption by creating a table and using the **Spark SQL** API to run a SQL statement that counts the number of late and non-late flights:

# COMMAND ----------

data.createOrReplaceTempView("flightData")
spark.sql("SELECT Late, COUNT(*) AS Count FROM flightData GROUP BY Late").show()

# COMMAND ----------

# MAGIC %md Yes, it looks like there are significantly more non-late flights than late ones - we can see this more clearly with a visualization, so let's use the inline **%sql** magic to query the table and bring back some results we can display as a chart:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Late, COUNT(*) AS Count FROM flightData GROUP BY Late

# COMMAND ----------

# MAGIC %sql
# MAGIC --notice you get the "display" for free with SparkSQL
# MAGIC select * from flightData

# COMMAND ----------

# MAGIC %md The query returns a table of data containing the first 1000 rows, which should be a big enough sample for us to explore. To see the distribution of *Late* classes (1 for late, 0 for on-time), in the visualization drop-down list under the table above, click **Bar**. Then click **Plot Options** and configure the visualization like this:
# MAGIC - **Keys**: Late
# MAGIC - **Series Groupings**: *none*
# MAGIC - **Values**: &lt;id&gt;
# MAGIC - **Aggregation**: Count
# MAGIC - **Display type**: Bar chart
# MAGIC - **Grouped**: Selected
# MAGIC 
# MAGIC You should be able to see that the sample includes significantly more on-time flights than late ones. This indicates that the dataset is *imbalanced*; which might adversely affect the accuracy of any machine learning model we train from this data.
# MAGIC 
# MAGIC Additionally, you observed earlier that there are some extremely high **DepDelay** and **ArrDelay** values that might be skewing the distribution of the data disproportionately because of a few *outliers*. Let's visualize the distribution of these columns to explore this. Change the **Plot Options** settings as follows:
# MAGIC - **Keys**: *none*
# MAGIC - **Series Groupings**: *none*
# MAGIC - **Values**: DepDelay
# MAGIC - **Aggregation**: Count
# MAGIC - **Display Type**: Histogram plot
# MAGIC - **Number of bins**: 20
# MAGIC 
# MAGIC You can drag the handle at the bottom right of the visualization to resize it. Note that the data is skewed such that most flights have a **DepDelay** value within 100 or so minutes of 0. However, there are a few flights with extremely high delays. Another way to view this distribution is a *box plot*. Change the **Plot Options** as follows:
# MAGIC - **Keys**: *none*
# MAGIC - **Series Groupings**: *none*
# MAGIC - **Values**: DepDelay
# MAGIC - **Aggregation**: Count
# MAGIC - **Display Type**: Box plot
# MAGIC 
# MAGIC The box plot consists of a box with a line indicating the median departure delay, and *whiskers* extending from the box to show the first and fourth quartiles of the data, with statistical *outliers* shown as small circles. This confirms the extremely skewed distribution of **DepDelay** values seen in the histogram (and if you care to check, you'll find that the **ArrDelay** column has a similar distribution).
# MAGIC 
# MAGIC Let's address the outliers and imbalanced classes in our data by removing rows with extreme delay values, and *undersampling* the more common on-time flights:

# COMMAND ----------

from pyspark.sql.functions import rand

# Remove outliers - let's make the cut-off 150 minutes.
data = data.filter("DepDelay < 150 AND ArrDelay < 150")

# Separate the late and on-time flights
pos = data.filter("Late = 1")
neg = data.filter("Late = 0")

# undersample the most prevalent class to get a roughly even distribution
posCount = pos.count()
negCount = neg.count()
if posCount > negCount:
  pos = pos.sample(True, negCount/(negCount + posCount))
else:
  neg = neg.sample(True, posCount/(negCount + posCount))
  
# shuffle into random order (so a sample of the first 1000 has a mix of classes)
data = neg.union(pos).orderBy(rand())

# Replace the temporary table so we can query and visualize the balanced dataset
data.createOrReplaceTempView("flightData")

# Show the statistics
display(data.describe())

# COMMAND ----------

# MAGIC %md Now the maximums for the **DepDelay** and **ArrDelay** are clipped at under 150, and the mean value for the binary *Late* class is nearer 0.5; indicating a more or less even number of each class. We removed some data to accomplish this balancing act, but there are still a substantial number of rows for us to train a machine learning model with, and now the data is more balanced. Let's visualize the data again to confirm this:

# COMMAND ----------

# MAGIC %md Display the data as a bar chart to compare the distribution of the **Late** classes as you did previously. There should now be a more or less even number of each class. Then visualize the **DepDelay** field as a histogram and as a box plot to verify that the distribution, while still skewed, has fewer outliers.

# COMMAND ----------

# MAGIC %md ### Explore Relationships in the Data
# MAGIC Predictive modeling is largely based on statistical relationships between fields in the data. To design a good model, you need to understand how the data points relate to one another.
# MAGIC 
# MAGIC A common way to start exploring relationships is to create visualizations that compare two or more data values. For example, modify the **Plot Options** of the chart above to compare the arrival delays for each carrier:
# MAGIC - **Keys**: Carrier
# MAGIC - **Series Groupings**: *none*
# MAGIC - **Values**: ArrDelay
# MAGIC - **Aggregation**: Count
# MAGIC - **Display Type**: Box plot
# MAGIC 
# MAGIC You may need to resize the plot to see the data clearly, but it should show that the median delay, and the distribution of delays varies by carrier; with some carriers having a higher median delay than others. The same is true for other features, such as the day of the week and the destination airport. You may already suspect that there's likely to be a relationship between delarture delay and arrival delay, so let's examine that next. Change the **Plot Options** as follows:
# MAGIC - **Keys**: None
# MAGIC - **Series Groupings**: *none*
# MAGIC - **Values**: ArrDelay, DepDelay
# MAGIC - **Aggregation**: Count
# MAGIC - **Display Type**: Scatter plot
# MAGIC - **Show LOESS**: Selected
# MAGIC 
# MAGIC The scatter plot shows the departure delay and corresponding arrival delay for each flight as a point in a two dimensional space. Note that the points form a diagonal line, which indicates a strong linear relationship between departure delay and arrival delay. This linear relationship shows a *correlation* between these two values, which we can measure statistically. The **corr** function calculates a correlation value between -1 and 1, indicating the strength of correlation between two fields. A strong positive correlation (near 1) indicates that high values for one column are often found with high values for the other, which a strong negative correlation (near -1) indicates that *low* values for one column are often found with *high* values for the other. A correlation near 0 indicates little apparent relationship between the fields.

# COMMAND ----------

data.corr("DepDelay", "ArrDelay")

# COMMAND ----------

# MAGIC %md In this notebook we've cleaned the flight data, and explored it to identify some potential relationships between features of the flights and their lateness.
