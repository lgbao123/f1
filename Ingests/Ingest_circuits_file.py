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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/circuits.csv'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/circuits'


# COMMAND ----------

#Define schema 
circuits_schema = StructType([
    StructField('circuitId',IntegerType(),False),
    StructField('circuitRef',StringType(),True),
    StructField('name',StringType(),True),
    StructField('location',StringType(),True),
    StructField('country',StringType(),True),
    StructField('lat',DoubleType(),True),
    StructField('lng',DoubleType(),True),
    StructField('alt',IntegerType(),True),
    StructField('url',StringType(),True)
])
#read file csv
circuits_df = spark.read.csv(input_path,header = True,schema = circuits_schema)

circuits_df.show(5)
circuits_df.printSchema()
                    

# COMMAND ----------

# transform
circuits_df = circuits_df\
                    .withColumnRenamed('circuitId','circuit_id')\
                    .withColumnRenamed('circuitRef','circuit_ref')\
                    .withColumnRenamed('lat','latitude')\
                    .withColumnRenamed('lng','longtitude')\
                    .withColumnRenamed('alt','altitude ')\
                    .drop('url')\
                    .withColumn('ingest_date',current_timestamp())


# COMMAND ----------

display(circuits_df)


# COMMAND ----------

# write to datalake
circuits_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))
