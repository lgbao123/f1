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

relative_path = f'/ErgastApi/Result/year={year}/month={month}/day={day}/result.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)
                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convention name , cast type , add audit col, exploded col

# COMMAND ----------

# transform
results_df = df \
                .select('MrData.RaceTable.Races') \
                .withColumn('Races',explode('Races')) \
                .withColumn('Results',explode('Races.Results')) \
                .select(
                    concat(regexp_replace(col('Races.raceName'),'[ ]+','_'),col('Races.round'),col('Races.season')).alias('race_id'),
                    col('Races.Circuit.circuitId').alias('circuit_id'),
                    col('Results.Constructor.constructorId').alias('constructor_id'),
                    col('Results.Driver.driverId').alias('driver_id'),
                    col('Results.Time.time').alias('total_time'),
                    col('Results.grid').cast('int').alias('grid'),
                    col('Results.laps').cast('int').alias('laps'),
                    col('Results.number').cast('int').alias('car_number'),
                    col('Results.points').cast('int').alias('points'),
                    col('Results.position').cast('int').alias('position'),
                    col('Results.status').alias('status'),
                ) \
                .withColumn('ingestion_date',lit(p_file_date).cast('date'))\

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find and remove duplicate

# COMMAND ----------

window= Window.partitionBy('race_id','driver_id').orderBy(col('constructor_id'))
check_dup =results_df.groupBy('race_id','driver_id').count().filter(col('count')>1)
display(check_dup)
results_df =results_df.withColumn('rn',row_number().over(window)).filter(col('rn')==1).drop('rn')
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null

# COMMAND ----------

checkNullDF(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data

# COMMAND ----------

# write to datalake
path =f'{processed_path}/results'
condition = '   tgt.constructor_id = up.constructor_id  and \
                tgt.driver_id = up.driver_id and \
                tgt.race_id = up.race_id and \
                tgt.circuit_id = up.circuit_id '
    
partitionOverwrite(df=results_df ,dbname='f1_processed',tablename='results',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from f1_processed.results
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_processed.results
# MAGIC -- where concat(driver_id,race_id) in 
# MAGIC select driver_id,race_id,count(*) 
# MAGIC     from f1_processed.results 
# MAGIC     group by driver_id,race_id
# MAGIC     having count(*) > 1
# MAGIC   