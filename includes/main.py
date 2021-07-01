# Databricks notebook source
# MAGIC %md
# MAGIC This notebook builds a composable software architecture to databricks data engineering using streaming.  We should be using `includes` files and `%run notebook` syntax to abstract away the implementation details of Spark Structured Streaming, which makes the notebooks much more easier to read for a business analyst.  
# MAGIC 
# MAGIC Consider structuring your includes like this
# MAGIC 
# MAGIC * includes folder
# MAGIC   * main:  this is the "driving" includes file.  This should be called by every notebook.  Something like this:  `%run ./Includes/main`.  It, in turn, calls:  
# MAGIC     * configuration:  declare your vars here.  Things like data lake locations, sql table names, etc.  We will (well, _should_) use this in our labs to ensure every attendee has unique databricks objects so we aren't stomping on each other.  
# MAGIC     * imports:  put your `imports` here.  This gives you a standard set of imports done in the same way for every notebook/project.  Code review these.  
# MAGIC     * utilities:  any code that you find you are reusing everywhere, put it here.  These should be organized as classes and functions.  Examples:  `delta-ops.optimize` will run the OPTIMIZE command on a given table(s)
# MAGIC     * operations:  or in this case, `stream-ops`.  This will hae all of the code I need to start, stop, readStream, and writeStream, using standard patterns we all agree to as a team, unit test, and code review.  

# COMMAND ----------

# MAGIC %run ./configuration

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./operations
