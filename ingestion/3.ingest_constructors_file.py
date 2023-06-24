# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constructor_df= spark.read \
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")
display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - drop unwanted columns from dataframe

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')
display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the required columns and add ingestion_date column 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructor_final_df= add_ingestion_date(constructor_dropped_df.withColumnRenamed("constructorID","constructor_id")\
.withColumnRenamed("constructorRef","constrcutor_ref").withColumn("data_source",lit(v_data_source)).withColumn("file_date", lit(v_file_date)))
display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adl/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!!")