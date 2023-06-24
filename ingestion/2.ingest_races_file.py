# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

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
# MAGIC #### Step-1 Read the CSV file using the spark dataframe reader 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/forumla1adl/raw

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema= StructType(fields=[StructField("raceId", IntegerType(),False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(),True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

races_df=spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select the required columns and rename them accordingly

# COMMAND ----------

races_selected_df=races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time"))
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add ingestion_date column and a new column according to your needs

# COMMAND ----------

races_final_df=add_ingestion_date(races_selected_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time') ), 'yyyy-MM-dd HH:mm:ss')))
    #.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

races_final_df=races_final_df.select(col("race_id"),col("race_year"), col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date")).withColumn("data_source",lit(v_data_source)).withColumn("file_date", lit(v_file_date))
display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")
#circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!!")

# COMMAND ----------

