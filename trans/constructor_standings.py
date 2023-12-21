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
results_df = spark.read.format('delta').load(f'{presentation_path}/race_results')
display(results_df)


# COMMAND ----------

#find race_year only on file date
results_df_1 = results_df.filter(results_df.file_date == p_file_date)
race_year_list = changeDistinctColumnToList(results_df_1,'race_year')

# COMMAND ----------

#filter race_results base on race_year
results_df =results_df.filter(col('race_year').isin(race_year_list))

# COMMAND ----------

# transform 
final_df = results_df.groupBy('race_year','team') \
                             .agg(sum('points').alias('total_points'),
                                  count(when(col('position')==1 , True)).alias('wins')
                                )\
                             .orderBy(desc('race_year'),desc('total_points'),desc('wins'))\
                             
                             



# COMMAND ----------

# write to datalake
path =f'{presentation_path}/constructor_standings'
condition = 'tgt.team = up.team and tgt.race_year = up.race_year '
partitionOverwrite(df=final_df ,dbname='f1_presentation',tablename='constructor_standings',parttion_column='race_year' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings

# COMMAND ----------

display(final_df)

# COMMAND ----------


