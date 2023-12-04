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
    StructField('lap',IntegerType(),True),
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
path =f'{processed_path}/pitstops'
condition = 'tgt.driver_id = up.driver_id and tgt.race_id = up.race_id and tgt.stop = up.stop'
partitionOverwrite(df=pitstops_df ,dbname='f1_processed',tablename='pitstops',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from f1_processed.pitstops
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------


