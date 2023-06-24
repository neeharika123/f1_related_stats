# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_schema = "resultId  INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT"

# COMMAND ----------

results_df= spark.read \
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - drop unwanted columns from dataframe

# COMMAND ----------

results_dropped_df=results_df.drop('statusId')
display(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the required columns and add ingestion_date column 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

results_final_df= add_ingestion_date(results_dropped_df.withColumnRenamed("resultId","result_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date)))
display(results_final_df)

# COMMAND ----------

results_deduped_df=results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#      if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id ={race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

merge_condition = "tgt.result_id=src.result_id and tgt.race_id=src.race_id"
merge_delta_date(results_deduped_df, "f1_processed", "results",processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

#overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by 1,2 having count(1)>1

# COMMAND ----------

dbutils.notebook.exit("SUCCESS!!")

# COMMAND ----------

