# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_result_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list=df_column_to_list(race_result_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import col
race_result_df=spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, col, when, count,desc
constructor_standings_df=race_result_df.\
groupBy("race_year","team").\
agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))
display(constructor_standings_df)
    

# COMMAND ----------

display(constructor_standings_df.filter("race_year=2020").orderBy(desc("total_points")))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# COMMAND ----------

constructorRankSpec=Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df=constructor_standings_df.withColumn("rank", rank().over(constructorRankSpec))
display(final_df.filter("race_year==2020"))
                                                       

# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
#races_circuits_results_drivers_constructors_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")


# COMMAND ----------

merge_condition = "tgt.race_year=src.race_year and tgt.team=src.team"
merge_delta_date(final_df, "f1_presentation", "constructor_standings",presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings order by race_year desc

# COMMAND ----------

