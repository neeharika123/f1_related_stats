# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying.json folder

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

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields= [StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")
display(qualifying_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new columns
# MAGIC 1. Rename qualifyId, raceId, driverId, constructorId
# MAGIC 1. add ingestion_date with current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date", lit(v_file_date)))
display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

merge_condition = "tgt.race_id=src.race_id and tgt.qualify_id=src.qualify_id"
merge_delta_date(qualifying_final_df, "f1_processed", "qualifying",processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

#overwrite_partition(qualifying_final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!!")