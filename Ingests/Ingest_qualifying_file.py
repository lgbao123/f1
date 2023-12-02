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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/qualifying'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/qualifying'


# COMMAND ----------

#Define schema 
qualifying_schema = StructType([
    StructField('qualifyId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('q1',StringType(),True),
    StructField('q2',StringType(),True),
    StructField('q3',StringType(),True),
])

#read file csv
# results_df = spark.read.json(input_path,schema =results_schema)
qualifying_df = spark.read.json(input_path,schema=qualifying_schema,multiLine=True)
                   

qualifying_df.show(5)
qualifying_df.printSchema()
                    

# COMMAND ----------

# transform
qualifying_df = qualifying_df\
                    .withColumnRenamed('qualifyId','qualify_id')\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumnRenamed('constructorId','constructor_id')\
                    .withColumn('ingest_date',current_timestamp())


# COMMAND ----------

display(qualifying_df)


# COMMAND ----------

# write to datalake
qualifying_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))
