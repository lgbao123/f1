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
input_path = 'abfss://raw@formula12609dl.dfs.core.windows.net/races.csv'
output_path ='abfss://processed@formula12609dl.dfs.core.windows.net/races'


# COMMAND ----------

#Define schema 
races_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('year',IntegerType(),True),
    StructField('round',IntegerType(),True),
    StructField('circuitId',IntegerType(),False),
    StructField('name',StringType(),True),
    StructField('date',DateType(),True),
    StructField('time',StringType(),True),
    StructField('url',StringType(),True)
])
#read file csv
races_df = spark.read.csv(input_path,header = True,schema =races_schema)

races_df.show(5)
races_df.printSchema()
                    

# COMMAND ----------

# transform
races_df = races_df\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('circuitId','circuit_id')\
                    .withColumnRenamed('year','race_year')\
                    .withColumn('race_timestamp',concat(col('date'),lit(' '),col('time')))\
                    .drop('url','date','time')\
                    .withColumn('ingest_date',current_timestamp())


# COMMAND ----------

display(races_df)


# COMMAND ----------

# write to datalake
races_df.write.mode('overwrite').partitionBy('race_year').parquet(output_path)

# COMMAND ----------

# display(dbutils.fs.ls(output_path))
