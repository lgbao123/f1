# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Set parameters
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')


# COMMAND ----------

#read dataframe 
results_df = spark.read.format('delta').load(f'{processed_path}/results')
drivers_df = spark.read.format('delta').load(f'{processed_path}/drivers')
constructors_df = spark.read.format('delta').load(f'{processed_path}/constructors')
races_df = spark.read.format('delta').load(f'{processed_path}/races')
circuits_df = spark.read.format('delta').load(f'{processed_path}/circuits')


# COMMAND ----------

results_df_filtered = results_df.filter(results_df['file_date']== p_file_date)
# display(results_df_filtered)

# COMMAND ----------

# transform 
races_join_df = races_df.join(circuits_df,[races_df.circuit_id == circuits_df.circuit_id]) \
                        .select(races_df['race_id'],races_df['name'].alias('race_name'), col('race_timestamp').alias('race_date'), 'race_year' ,'location')
races_join_df.show(5)


# COMMAND ----------

final_df = results_df_filtered.join(races_join_df , [results_df_filtered.race_id == races_join_df.race_id])\
                    .join(drivers_df , [results_df_filtered.driver_id == drivers_df.driver_id])\
                    .join(constructors_df , [results_df_filtered.constructor_id == constructors_df.constructor_id])\
                    .select('race_year','race_name','race_date','location',drivers_df.driver_id,drivers_df.name.alias('driver_name') , drivers_df.number.alias('driver_number') ,drivers_df.nationality.alias('driver_nationality'),constructors_df.name.alias('team'),'grid','fastest_lap','time','points','position',results_df_filtered.race_id ,results_df_filtered.file_date)\
                    .withColumn('created_date',current_timestamp()) \
                    .orderBy(col('race_year').desc(),col('points').desc())


# COMMAND ----------

display(final_df)

# COMMAND ----------

# write to datalake
path =f'{presentation_path}/race_results'
condition = 'tgt.driver_id = up.driver_id and tgt.race_id = up.race_id and tgt.team = up.team and tgt.race_year = up.race_year '
partitionOverwrite(df=final_df ,dbname='f1_presentation',tablename='race_results',parttion_column='race_year' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id , count(1) from f1_presentation.race_results
# MAGIC group by 1 
# MAGIC order by 1 desc;
# MAGIC

# COMMAND ----------


