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
input_path = f'{raw_path}/{p_file_date}/qualifying'
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
                    .withColumn('ingest_date',current_timestamp())\
                    .withColumn('file_date',lit(p_file_date))



# COMMAND ----------

display(qualifying_df)


# COMMAND ----------

# write to datalake
qualifying_df.write.mode('append').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.qualifying;

# COMMAND ----------


