# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Prinicpal
# MAGIC 1. Register Azure AD Application/Service Principal
# MAGIC 1. Generate a secret/password for the application
# MAGIC 1. Set spark config with App/ Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake 

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope' , key = 'formula1-adl-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.forumla1adl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.forumla1adl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.forumla1adl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.forumla1adl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.forumla1adl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token" )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@forumla1adl.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@forumla1adl.dfs.core.windows.net/circuits.csv",header=True))

# COMMAND ----------

