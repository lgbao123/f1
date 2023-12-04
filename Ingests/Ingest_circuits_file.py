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

input_path = f'{raw_path}/{p_file_date}/circuits.csv'

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
                    .withColumn('ingest_date',current_timestamp()) \
                    .withColumn('file_date',lit(p_file_date)) 


# COMMAND ----------

display(circuits_df)


# COMMAND ----------

# write to datalake
circuits_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.circuits");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits ; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc extended f1_processed.circuits ;

# COMMAND ----------


