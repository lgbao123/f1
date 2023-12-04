# Databricks notebook source
# MAGIC %run ../Includes/configuration

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
input_path = f'{raw_path}/{p_file_date}/pit_stops.json'
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
                    .withColumn('ingest_date',current_timestamp())\
                    .withColumn('file_date',lit(p_file_date))


# COMMAND ----------

display(pitstops_df)


# COMMAND ----------

# write to datalake
pitstops_df.write.mode('append').format('parquet').saveAsTable('f1_processed.pitstops')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops
