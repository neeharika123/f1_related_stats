# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from  circuits.csv file

# COMMAND ----------

formula1_adl_sas_token = dbutils.secrets.get(scope = 'formula1-scope' , key = 'forumula1adl-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.forumla1adl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.forumla1adl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.forumla1adl.dfs.core.windows.net", formula1_adl_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1adl.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1adl.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

