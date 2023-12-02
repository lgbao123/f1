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
# Path
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/results.json'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/results'


# COMMAND ----------

#Define schema 
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
                    .drop('statusId')


# COMMAND ----------

display(results_df)


# COMMAND ----------

# write to datalake
results_df.write.mode('overwrite').partitionBy('race_id').parquet(output_path)

# COMMAND ----------

# display(dbutils.fs.ls(output_path))
