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
results_df = spark.read.parquet(f'{presentation_path}/race_results')
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
                             
                             

display(final_df)


# COMMAND ----------

# Write df 
partitionOverwrite(dbname='f1_presentation',tablename='constructor_standings',df= final_df ,parttion_column='race_year')
