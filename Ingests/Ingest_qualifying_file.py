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
path =f'{processed_path}/qualifying'
condition = 'tgt.qualify_id = up.qualify_id and tgt.race_id = up.race_id'
partitionOverwrite(df=qualifying_df ,dbname='f1_processed',tablename='qualifying',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from f1_processed.qualifying
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------


