# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Parameter
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

#Define schema 
input_path = f'{raw_path}/{p_file_date}/lap_times'
lap_times_schema = StructType([
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),False),
    StructField('lap',IntegerType(),False),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True)
])

#read file csv
lap_times_df = spark.read.csv(input_path,schema=lap_times_schema)
                   

lap_times_df.show(5)
lap_times_df.printSchema()
                    

# COMMAND ----------

# transform
lap_times_df = lap_times_df\
                    .withColumnRenamed('raceId','race_id')\
                    .withColumnRenamed('driverId','driver_id')\
                    .withColumn('ingest_date',current_timestamp())\
                    .withColumn('file_date',lit(p_file_date))


# COMMAND ----------

display(lap_times_df)


# COMMAND ----------

# write to datalake
lap_times_df.write.mode('append').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------


