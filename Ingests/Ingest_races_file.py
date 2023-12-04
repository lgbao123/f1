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
input_path = f'{raw_path}/{p_file_date}/races.csv'
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
                    .withColumn('ingest_date',current_timestamp())\
                    .withColumn('file_date',lit(p_file_date)) \
                    .drop('url','date','time')



# COMMAND ----------

display(races_df)


# COMMAND ----------

# write to datalake
output_path = f"{processed_path}/races"
races_df.write.mode("overwrite").partitionBy("race_year").format('parquet').saveAsTable("f1_processed.races",path = output_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races  ;
