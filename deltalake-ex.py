# Databricks notebook source
spark.conf.set(
   "fs.azure.account.key.serviceaccountdatabricks.dfs.core.windows.net",
   dbutils.secrets.get(scope="databricks-kv-test",
   key="serviceaccountdatabricks-adls2-secret"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
display(dbutils.fs.ls("abfss://databricksadls@serviceaccountdatabricks.dfs.core.windows.net/"))

# COMMAND ----------

# Read data from databricks 
read_format = 'delta'
load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
 
# Load the data from its source.
people = spark.read \
  .format(read_format) \
  .load(load_path)
 
# Show the results.
display(people)


# COMMAND ----------

# Define the output format, output mode, columns to partition by, and the output path.

write_format = 'delta'
write_mode = 'overwrite'
partition_by_gender = 'gender,ssn'
partition_by_ssn = 'ssn'
save_path = 'abfss://databricksadls@serviceaccountdatabricks.dfs.core.windows.net/people1/'
 
# Write the data to its target.
people.write \
  .format(write_format) \
  .partitionBy('gender') \
  .mode(write_mode) \
  .save(save_path)

# COMMAND ----------

read_format = 'delta'
people_delta = spark.read \
  .format(read_format) \
  .load('abfss://databricksadls@serviceaccountdatabricks.dfs.core.windows.net/people1/')
display(people_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC Create temporary table

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("CREATE TABLE people USING DELTA LOCATION 'abfss://databricksadls@serviceaccountdatabricks.dfs.core.windows.net/people1/' ")

# COMMAND ----------

display(spark.sql("select * from people where firstName = 'Pennie' and lastName = 'Hirschmann' "))

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("update people set salary = 11112 where  firstName = 'Pennie' and lastName = 'Hirschmann' ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from people where  firstName = 'Pennie' and lastName = 'Hirschmann'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists people

# COMMAND ----------

people_delta.count() 

# COMMAND ----------

display(spark.table("people").select("salary").orderBy("salary", ascending = False) )

# COMMAND ----------


