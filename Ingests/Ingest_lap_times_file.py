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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/lap_times'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/lap_times'


# COMMAND ----------

#Define schema 
lap_times_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),False),
    StructField('lap',IntegerType(),False),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

#read file csv
# results_df = spark.read.json(input_path,schema =results_schema)
lap_times_df = spark.read.csv(input_path,schema=lap_times_schema)
                   

lap_times_df.show(5)
lap_times_df.printSchema()
                    

# COMMAND ----------

# transform
lap_times_df = lap_times_df\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumn('ingest_date',current_timestamp())


# COMMAND ----------

display(lap_times_df)


# COMMAND ----------

# write to datalake
lap_times_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))

# COMMAND ----------


