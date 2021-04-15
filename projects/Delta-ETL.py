# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Delta
# MAGIC 
# MAGIC In this lab, we will complete the following in **batch** (but we could easily stream this too):<br>
# MAGIC 1.  Create a dataset, persist in Delta format to DBFS backed by Azure Blob Storage, create a Delta table on top of the dataset<br>
# MAGIC 2.  Update one or two random records<br>
# MAGIC 3.  Delete one or two records<br>
# MAGIC 4.  Add a couple new columns to the data and understand considerations for schema evolution<br>
# MAGIC 5.  Discuss Databricks Delta's value proposition, and positioning in your big data architecture<br>
# MAGIC 6.  Understand various DBA tasks you have to do with Delta <br>
# MAGIC 
# MAGIC References:
# MAGIC https://docs.azuredatabricks.net/delta/index.html<br>

# COMMAND ----------

# this shouldn't be needed anymore
#spark.conf.set("spark.databricks.delta.preview.enabled", True) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.0. Basic create operation

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.0.1. Create some data

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901)
]
booksDF = spark.createDataFrame(vals, columns)
booksDF.printSchema
display(booksDF)

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.2. Persist to Delta format

# COMMAND ----------

# Destination directory for Delta table
# this folder doesn't actually exist anywhere, that's ok.  
deltaTableDirectory = "/mnt/delta-exploration"
dbutils.fs.rm(deltaTableDirectory, recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists books_db.books;

# COMMAND ----------

# Persist dataframe to delta format without coalescing
# syntactically this is identical to writing to parquet
booksDF.write.format("delta").save(deltaTableDirectory)

# COMMAND ----------

# note that the output looks just like parquet as well, except for the _delta_log
display(dbutils.fs.ls(deltaTableDirectory))

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.3. Create Delta table
# MAGIC  This is a Spark SQL "unmanaged" table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS books_db;
# MAGIC 
# MAGIC USE books_db;
# MAGIC DROP TABLE IF EXISTS books;
# MAGIC CREATE TABLE books
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/delta-exploration";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC  %md
# MAGIC  #### 1.0.4. Performance optimization
# MAGIC  We will run the "OPTIMIZE" command to compact small files into larger for performance.
# MAGIC  Note: The performance improvements are evident at scale

# COMMAND ----------

# 1) Lets look at part file count, how many are there? In my case, 6
display(dbutils.fs.ls(deltaTableDirectory))

# COMMAND ----------

# 2) Lets read the dataset and check the partition size, it should be the same as number of small files
preDeltaOptimizeDF = spark.sql("select * from books_db.books")
preDeltaOptimizeDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;
# MAGIC --3) Lets run DESCRIBE DETAIL 
# MAGIC --Notice that numFiles = 6

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Now, lets run optimize
# MAGIC USE books_db;
# MAGIC OPTIMIZE books;

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;
# MAGIC --5) Notice the number of files now - its 1 file

# COMMAND ----------

# 6) Lets look at the part file count, it has actually added a file!!!
# Guess why?
display(dbutils.fs.ls(deltaTableDirectory))

# COMMAND ----------

#7) Lets read the dataset and check the partition size, it should be the same as number of small files
postDeltaOptimizeDF = spark.sql("select * from books_db.books")
postDeltaOptimizeDF.rdd.getNumPartitions()
#Its 1, and not 7
#Guess why?

# COMMAND ----------

# MAGIC %sql 
# MAGIC --8a) Lets do some housekeeping now
# MAGIC --we need to set this otherwise databricks complains that we shouldn't purge recent history data
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC --8b) Run vacuum
# MAGIC VACUUM books_db.books retain 0 hours;

# COMMAND ----------

# 9) Lets look at the part file count, its 1 now!
display(dbutils.fs.ls(deltaTableDirectory))

# COMMAND ----------

#10) Lets read the dataset and check the partition size, again now
postDeltaOptimizeDF = spark.sql("select * from books_db.books")
postDeltaOptimizeDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **Learnings:**<br>
# MAGIC OPTIMIZE compacts small files into larger ones but does not do housekeeping.<br>
# MAGIC VACUUM does the housekeeping of the small files from prior to compaction, and more.<br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.0. Append operation

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1. Create some data to add to the table & persist it

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksDF = spark.createDataFrame(vals, columns)
booksDF.printSchema
display(booksDF)

# COMMAND ----------

booksDF.write.format("delta").mode("append").save(deltaTableDirectory)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2. Lets query the table without making any changes or running table refresh<br>
# MAGIC We should see the new book entries.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC 2.3. Under the hood..

# COMMAND ----------

# MAGIC %sql DESCRIBE DETAIL books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE books_db.books;
# MAGIC VACUUM books_db.books;

# COMMAND ----------

# notice that numFiles has decreased
%sql DESCRIBE DETAIL books_db.books;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3.0. Update/upsert operation

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
       ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887),
       ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890),
       ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
       ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
       ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901),
       ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905),
       ("b00909", "Sir Arthur Conan Doyle", "A scandal in Bohemia", 1891),
       ("b00223", "Sir Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksUpsertDF = spark.createDataFrame(vals, columns)
booksUpsertDF.printSchema
display(booksUpsertDF)

# COMMAND ----------

# 2) Create a temporary view on the upserts
booksUpsertDF.createOrReplaceTempView("books_upserts")

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Execute upsert
# MAGIC USE books_db;
# MAGIC 
# MAGIC MERGE INTO books
# MAGIC USING books_upserts
# MAGIC ON books.book_id = books_upserts.book_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     books.book_author = books_upserts.book_author,
# MAGIC     books.book_name = books_upserts.book_name,
# MAGIC     books.book_pub_year = books_upserts.book_pub_year
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (book_id, book_author, book_name, book_pub_year) VALUES (books_upserts.book_id, books_upserts.book_author, books_upserts.book_name, books_upserts.book_pub_year);

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Validate
# MAGIC Select * from books_db.books;

# COMMAND ----------

# 5) Files? How many?
display(dbutils.fs.ls(deltaTableDirectory))

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 6) What does describe detail say?
# MAGIC DESCRIBE DETAIL books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7) Lets optimize 
# MAGIC OPTIMIZE books_db.books;
# MAGIC VACUUM books_db.books;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4.0. Delete operation

# COMMAND ----------

# MAGIC %sql
# MAGIC --1) Lets isolate records to delete
# MAGIC select * from books_db.books where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --2) Execute delete
# MAGIC USE books_db;
# MAGIC 
# MAGIC DELETE FROM books where book_pub_year >= 1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Lets validate
# MAGIC select * from books_db.books where book_pub_year>=1900;

# COMMAND ----------

# MAGIC %sql
# MAGIC --4) Lets validate further
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.0. Overwrite
# MAGIC Works only when there are no schema changes

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900)
]
booksOverwriteDF = spark.createDataFrame(vals, columns)
booksOverwriteDF.printSchema
display(booksOverwriteDF)

# COMMAND ----------

# 2) Overwrite the table
booksOverwriteDF.write.format("delta").mode("overwrite").save(deltaTableDirectory)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC -- Notice the "- the great" in the name in the couple records?
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. Automatic schema update

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year", "book_price"]
vals = [
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887, 2.33),
     ("b00001", "Arthur Conan Doyle", "A study in scarlet", 1887, 5.12),
     ("b01001", "Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 12.00),
     ("b00501", "Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 13.39),
     ("b00300", "Arthur Conan Doyle", "The hounds of Baskerville", 1901, 22.00),
     ("b00909", "Arthur Conan Doyle", "A scandal in Bohemia", 1891, 18.00),
     ("b00023", "Arthur Conan Doyle", "Playing with Fire", 1900, 29.99)
]
booksNewColDF = spark.createDataFrame(vals, columns)
booksNewColDF.printSchema
display(booksNewColDF)

# COMMAND ----------

booksNewColDF.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(deltaTableDirectory)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Query
# MAGIC select * from books_db.books;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE books_db;
# MAGIC 
# MAGIC DELETE FROM books where book_price is null;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0. Partitioning
# MAGIC Works just the same as usual

# COMMAND ----------

columns = ["book_id", "book_author", "book_name", "book_pub_year", "book_price"]
vals = [
       ("b00001", "Sir Arthur Conan Doyle", "A study in scarlet", 1887, 5.33),
       ("b00023", "Sir Arthur Conan Doyle", "A sign of four", 1890, 6.00),
       ("b01001", "Sir Arthur Conan Doyle", "The adventures of Sherlock Holmes", 1892, 2.99),
       ("b00501", "Sir Arthur Conan Doyle", "The memoirs of Sherlock Holmes", 1893, 8.00),
       ("b00300", "Sir Arthur Conan Doyle", "The hounds of Baskerville", 1901, 4.00),
       ("b09999", "Sir Arthur Conan Doyle", "The return of Sherlock Holmes", 1905, 2.22),
       ("c09999", "Jules Verne", "Journey to the Center of the Earth ", 1864, 2.22),
        ("d09933", "Jules Verne", "The return of Sherlock Holmes", 1870, 3.33),
        ("f09945", "Jules Verne", "Around the World in Eighty Days", 1873, 4.44)
]
booksPartitionedDF = spark.createDataFrame(vals, columns)
booksPartitionedDF.printSchema
display(booksPartitionedDF)

# COMMAND ----------

partitionDirectory = deltaTableDirectory + 'books-part'

# COMMAND ----------

# 2) Persist
dbutils.fs.rm(partitionDirectory, recurse=True)
booksPartitionedDF.write.format("delta").partitionBy("book_author").save(partitionDirectory)

# COMMAND ----------

display(dbutils.fs.ls(partitionDirectory))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Create table
# MAGIC USE books_db;
# MAGIC DROP TABLE IF EXISTS books_part;
# MAGIC CREATE TABLE books_part
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/delta-explorationbooks-part";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) Seamless
# MAGIC select * from books_db.books_part;

# COMMAND ----------

# MAGIC %md
# MAGIC Visit the portal and take a look at the storage account to see how its organized.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.0. History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY books_db.books;
