# Databricks notebook source
# MAGIC %run ../Includes/configuration

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

relative_path = f'/ErgastApi/Race/year={year}/month={month}/day={day}/race.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)
                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### convention name, cast type , add audit col, add race_Id

# COMMAND ----------

# transform
races_df = df\
            .select('MRData.RaceTable.Races')\
            .withColumn('Races', explode('Races')) \
            .select(
                concat(regexp_replace(col('Races.raceName'),'[ ]+','_'),col('Races.round'),col('Races.season')).alias('race_id'),
                col('Races.Circuit.circuitId').alias('circuit_id'),
                col('Races.date').cast('date').alias('race_date'),
                col('Races.time').alias('race_time'),
                col('Races.raceName').alias('race_name'),
                col('Races.round').cast('int').alias('round'),
                col('Races.season').cast('int').alias('season')
            ) \
            .withColumn('ingestion_date',lit(p_file_date).cast('date')) \



# COMMAND ----------

display(races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### find and remove dupliacate

# COMMAND ----------

check_dup =  races_df.groupBy('race_id').count().filter(col('count') > 1)
display(check_dup)
window= Window.partitionBy('race_id').orderBy(col('race_date').desc())
races_df =races_df.withColumn('rn',row_number().over(window)).filter(col('rn')==1).drop('rn')
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null

# COMMAND ----------

checkNullDF(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write file

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# write to datalake
# output_path = f"{processed_path}/races"
races_df.write.mode("overwrite").partitionBy("season").format('delta').option('path',f'{processed_path}/races').saveAsTable("hive_metastore.f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.races  ;

# COMMAND ----------

