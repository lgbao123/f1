# Databricks notebook source
# MAGIC %run ../Includes/configuration
# MAGIC

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Parameter
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

#Define schema 
input_path = f'{raw_path}/{p_file_date}/results.json'
results_schema = StructType([
    StructField('resultId',IntegerType(),False),
    StructField('constructorId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('fastestLap',IntegerType(),True),
    StructField('fastestLapSpeed',DoubleType(),True),
    StructField('fastestLapTime',StringType(),True),
    StructField('grid',IntegerType(),True),
    StructField('laps',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('points',DoubleType(),True),
    StructField('position',IntegerType(),True),
    StructField('positionOrder',IntegerType(),True),
    StructField('positionText',StringType(),True),
    StructField('raceId',IntegerType(),True),
    StructField('rank',IntegerType(),True),
    StructField('statusId',IntegerType(),True),

])
#read file csv
# results_df = spark.read.json(input_path,schema =results_schema)
results_df = spark.read.json(input_path,schema=results_schema)

results_df.show(5)
results_df.printSchema()
                    

# COMMAND ----------

# transform
results_df = results_df\
                    .withColumnRenamed('constructorId','constructor_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumnRenamed('fastestLap','fastest_lap')\
                    .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                    .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                    .withColumnRenamed('positionOrder','position_order')\
                    .withColumnRenamed('positionText','position_text')\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('resultId','result_id')\
                    .withColumn('ingest_date',current_timestamp())\
                    .withColumn('file_date',lit(p_file_date))\
                    .drop('statusId')


# COMMAND ----------

display(results_df)


# COMMAND ----------

# write to datalake
path =f'{processed_path}/results'
condition = 'tgt.result_id = up.result_id and tgt.race_id = up.race_id'
partitionOverwrite(df=results_df ,dbname='f1_processed',tablename='results',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from f1_processed.results
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------


