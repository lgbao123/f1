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
results_df = spark.read.parquet(f'{processed_path}/results')
drivers_df = spark.read.parquet(f'{processed_path}/drivers')
constructors_df = spark.read.parquet(f'{processed_path}/constructors')
races_df = spark.read.parquet(f'{processed_path}/races')
circuits_df = spark.read.parquet(f'{processed_path}/circuits')


# COMMAND ----------

# transform 
races_join_df = races_df.join(circuits_df,[races_df.circuit_id == circuits_df.circuit_id]) \
                        .select(races_df['race_id'],races_df['name'].alias('race_name'), col('race_timestamp').alias('race_date'), 'race_year' ,'location')
races_join_df.show(5)


# COMMAND ----------

final_df = results_df.join(races_join_df , [results_df.race_id == races_df.race_id])\
                    .join(drivers_df , [results_df.driver_id == drivers_df.driver_id])\
                    .join(constructors_df , [results_df.constructor_id == constructors_df.constructor_id])\
                    .select('race_year','race_name','race_date','location',drivers_df.name.alias('driver_name') , drivers_df.number.alias('driver_number') ,drivers_df.nationality.alias('driver_nationality') \
                        ,constructors_df.name.alias('team'),'grid','fastest_lap','time','points','position')\
                    .withColumn('created date',current_timestamp()) \
                    .orderBy(col('race_year').desc(),col('points').desc())
display(final_df)

# COMMAND ----------

# Write df 
output_path =f'{presentation_path}/race_results'
final_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

# display(dbutils.fs.ls(output_path))
# display(spark.read.parquet(output_path))

# COMMAND ----------


