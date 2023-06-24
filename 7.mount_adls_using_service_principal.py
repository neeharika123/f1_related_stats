# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Prinicpal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 1. Set spark config with App/ Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@forumla1adl.dfs.core.windows.net/",
  mount_point = "/mnt/formula1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1/demo"))


# COMMAND ----------

display(spark.read.csv("/mnt/formula1/demo/circuits.csv",header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

