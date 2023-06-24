# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from  circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1adl.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1adl.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

