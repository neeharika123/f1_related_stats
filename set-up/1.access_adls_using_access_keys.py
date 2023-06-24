# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from  circuits.csv file

# COMMAND ----------

formula1adl_access_key=dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-access-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.forumla1adl.dfs.core.windows.net",
               formula1adl_access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1adl.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1adl.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

