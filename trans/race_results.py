# Databricks notebook source
# MAGIC %run ../Includes/configuration
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
# Parameter
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')
year = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%Y')
month = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%m')
day = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%d')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data 

# COMMAND ----------

#read dataframe 
results_df = spark.read.format('delta').load(f'{processed_path}/results')
drivers_df = spark.read.format('delta').load(f'{processed_path}/drivers')
constructors_df = spark.read.format('delta').load(f'{processed_path}/constructors')
races_df = spark.read.format('delta').load(f'{processed_path}/races')
circuits_df = spark.read.format('delta').load(f'{processed_path}/circuits')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Get new result_race from ingestion date 

# COMMAND ----------

results_df_filtered = results_df.filter(results_df['ingestion_date']== p_file_date)
display(results_df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join race table with circuit table

# COMMAND ----------

# transform 
races_join_df = races_df.join(circuits_df,[races_df.circuit_id == circuits_df.circuit_id]) \
                        .select(races_df['race_id'],'race_name', 'race_date', 'season','round' ,'circuit_name','circuit_country','circuit_locality')
races_join_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join result table with the orthers

# COMMAND ----------

final_df = results_df_filtered \
                    .join(races_join_df , [results_df_filtered.race_id == races_join_df.race_id])\
                    .join(drivers_df , [results_df_filtered.driver_id == drivers_df.driver_id])\
                    .join(constructors_df , [results_df_filtered.constructor_id == constructors_df.constructor_id])\
                    .select('season','round','race_name','race_date','circuit_country','circuit_locality',
                        concat(col('given_name'),lit(' '),col('family_name')).alias('driver_name') ,
                        'driver_nationality','car_number','constructors_name',
                        'grid','laps','total_time','points','position','status',
                        results_df_filtered.race_id ,results_df_filtered.constructor_id,
                        results_df_filtered.driver_id,results_df_filtered.circuit_id,
                        results_df_filtered.ingestion_date
                    )\
                    .withColumn('created_date',current_timestamp()) \
                    .orderBy(col('season').desc(),col('points').desc())


# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data

# COMMAND ----------

# write to datalake
path =f'{presentation_path}/race_results'
condition = 'tgt.driver_id = up.driver_id and \
            tgt.constructor_id = up.constructor_id  and \
            tgt.circuit_id = up.circuit_id and \
            tgt.race_id = up.race_id '
partitionOverwrite(df=final_df ,dbname='f1_presentation',tablename='race_results',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id , count(1) from hive_metastore.f1_presentation.race_results
# MAGIC group by 1 
# MAGIC order by 1 desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table hive_metastore.f1_presentation.results