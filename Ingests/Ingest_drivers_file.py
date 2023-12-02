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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/drivers.json'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/drivers'


# COMMAND ----------

#Define schema 
drivers_schema = StructType([
    StructField('code',StringType(),False),
    StructField('dob',DateType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('driverRef',StringType(),True),
    StructField('name',StructType([
        StructField('forename',StringType(),True),
        StructField('surname',StringType(),True)
    ]),True),
    StructField('nationality',StringType(),True),
    StructField('number',IntegerType(),True),
    StructField('url',StringType(),True),
])
#read file csv
# constructors_df = spark.read.json(input_path,schema=constructors_schema)
drivers_df = spark.read.json(input_path,schema=drivers_schema)

drivers_df.show(5)
drivers_df.printSchema()
                    

# COMMAND ----------

# transform
drivers_df = drivers_df\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumnRenamed('driverRef','driver_ref')\
                    .withColumn('name',concat('name.forename',lit(' '),'name.surname'))\
                    .withColumn('ingest_date',current_timestamp())\
                    .drop('url')


# COMMAND ----------

display(drivers_df)


# COMMAND ----------

# write to datalake
drivers_df.write.mode('overwrite').parquet(output_path)

# COMMAND ----------

display(dbutils.fs.ls(output_path))
