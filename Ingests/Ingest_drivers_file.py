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
input_path = f'{raw_path}/{p_file_date}/drivers.json'
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
                    .withColumn('file_date',lit(p_file_date))\
                    .drop('url')


# COMMAND ----------

display(drivers_df)


# COMMAND ----------

# write to datalake
drivers_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.drivers
