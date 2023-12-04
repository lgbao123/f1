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
path =f'{processed_path}/lap_times'
condition = 'tgt.driver_id = up.driver_id and tgt.race_id = up.race_id and tgt.lap = up.lap'
partitionOverwrite(df=lap_times_df ,dbname='f1_processed',tablename='lap_times',parttion_column='race_id' ,path=path , condition=condition )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit('success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id ,count(1) from f1_processed.pitstops
# MAGIC group by 1 
# MAGIC order by 1 desc;

# COMMAND ----------


