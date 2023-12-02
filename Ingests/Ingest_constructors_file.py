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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/constructors.json'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/constructors'


# COMMAND ----------

#Define schema 
constructors_schema = StructType([
    StructField('constructorId',IntegerType(),False),
    StructField('constructorRef',StringType(),True),
    StructField('name',StringType(),True),
    StructField('nationality',StringType(),True),
    StructField('url',StringType(),False),
])
#read file csv
constructors_df = spark.read.json(input_path,schema=constructors_schema)

constructors_df.show(5)
constructors_df.printSchema()
                    

# COMMAND ----------

# transform
constructors_df = constructors_df\
                    .withColumnRenamed('constructorId','constructor_id')\
                    .withColumnRenamed('constructorRef','constructor_ref')\
                    .withColumn('ingest_date',current_timestamp())\
                    .drop('url')


# COMMAND ----------

display(constructors_df)


# COMMAND ----------

# write to datalake
constructors_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))
