# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv files using the spark dataframe reader API

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                       ])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")
display(lap_times_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new columns
# MAGIC 1. Rename raceId and driverId
# MAGIC 1. add ingestion_date with current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id").withColumn("data_source",lit(v_data_source)).withColumn("file_date", lit(v_file_date)))
display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

merge_condition = "tgt.race_id=src.race_id and tgt.driver_id=src.driver_id and tgt.lap=src.lap"
merge_delta_date(lap_times_final_df, "f1_processed", "lap_times",processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

#overwrite_partition(lap_times_final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!!")