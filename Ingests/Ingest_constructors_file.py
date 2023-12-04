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
input_path = f'{raw_path}/{p_file_date}/constructors.json'
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
                    .withColumn('file_date',lit(p_file_date))\
                    .drop('url')


# COMMAND ----------

display(constructors_df)


# COMMAND ----------

# write to datalake
constructors_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------


