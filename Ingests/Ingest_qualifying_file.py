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
# MAGIC #### read data

# COMMAND ----------

relative_path = f'/ErgastApi/Qualifying/year={year}/month={month}/day={day}/qualifying.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)
                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convention name , cast type , add audit col, exploded col

# COMMAND ----------

# transform
qualifying_df = df\
                .select('MrData.RaceTable.Races') \
                .withColumn('Races',explode('Races')) \
                .withColumn('Results',explode('Races.QualifyingResults')) \
                .select(
                    concat(regexp_replace(col('Races.raceName'),'[ ]+','_'),col('Races.round'),col('Races.season')).alias('race_id'),
                    col('Races.Circuit.circuitId').alias('circuit_id'),
                    col('Results.Constructor.constructorId').alias('constructor_id'),
                    col('Results.Driver.driverId').alias('driver_id'),
                    col('Results.Q1').alias('q1'),
                    col('Results.Q2').alias('q2'),
                    col('Results.Q3').alias('q3'),
                    col('Results.number').cast('int').alias('car_number'),
                    col('Results.position').cast('int').alias('position'),
                ) \
                .withColumn('ingestion_date',lit(p_file_date).cast('date'))\



# COMMAND ----------

display(qualifying_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### find duplicate and remove 

# COMMAND ----------

window= Window.partitionBy('race_id','driver_id').orderBy(col('constructor_id'))
check_dup =qualifying_df.groupBy('race_id','driver_id').count().filter(col('count')>1)
display(check_dup)
qualifying_df =qualifying_df.withColumn('rn',row_number().over(window)).filter(col('rn')==1).drop('rn')
display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null

# COMMAND ----------

checkNullDF(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data

# COMMAND ----------

# write to datalake
path =f'{processed_path}/qualifying'
condition = 'tgt.driver_id = up.driver_id \
            tgt.constructor_id = up.constructor_id  \
            tgt.race_id = up.race_id \
            tgt.circuit_id = up.circuit_id' 
partitionOverwrite(df=qualifying_df ,dbname='f1_processed',tablename='qualifying',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from hive_metastore.f1_processed.qualifying
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------

