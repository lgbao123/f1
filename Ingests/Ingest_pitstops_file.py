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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/pit_stops.json'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/pitstops'


# COMMAND ----------

#Define schema 
pitstops_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),True),
    StructField('stop',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('duration',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

#read file csv
# results_df = spark.read.json(input_path,schema =results_schema)
pitstops_df = spark.read.schema(pitstops_schema).option('multiline',True).json(input_path)
                   

pitstops_df.show(5)
pitstops_df.printSchema()
                    

# COMMAND ----------

# transform
pitstops_df = pitstops_df\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumn('ingest_date',current_timestamp())


# COMMAND ----------

display(pitstops_df)


# COMMAND ----------

# write to datalake
pitstops_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))
