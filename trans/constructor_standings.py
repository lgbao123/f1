# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Set access key 
spark.conf.set(
    f"fs.azure.account.key.{acc_name}.dfs.core.windows.net",
    acc_access_token
)


# COMMAND ----------

#read dataframe 
results_df = spark.read.parquet(f'{presentation_path}/race_results')
display(results_df)


# COMMAND ----------

# transform 
final_df = results_df.groupBy('race_year','team') \
                             .agg(sum('points').alias('total_points'),
                                  count(when(col('position')==1 , True)).alias('wins')
                                )\
                             .orderBy(desc('race_year'),desc('total_points'),desc('wins'))\
                             
                             

display(final_df)


# COMMAND ----------

# Write df 
output_path =f'{presentation_path}/constructor_standings'
final_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

# display(dbutils.fs.ls(output_path))
# display(spark.read.parquet(output_path))
