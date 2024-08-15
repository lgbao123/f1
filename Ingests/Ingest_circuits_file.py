# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# Library
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
# Parameter
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')
year = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%Y')
month = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%m')
day = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%d')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

relative_path = f'/ErgastApi/Circuit/year={year}/month={month}/day={day}/circuit.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exploded, convention name, add audit column into data

# COMMAND ----------

df1 = df.select(col('MRData.CircuitTable.*')) \
        .withColumn('Circuits',explode('Circuits')) \
        .select(
                col('Circuits.circuitId').alias('circuit_id'),
                col('Circuits.circuitName').alias('circuit_name'),
                col('Circuits.Location.country').alias('circuit_country'),
                col('Circuits.Location.locality').alias('circuit_locality'),
                col('Circuits.Location.lat').alias('lat'),
                col('Circuits.Location.long').alias('long'),
                col('Circuits.url').alias('url')
        ) \
        .withColumn('ingestion_date',lit(p_file_date).cast('date')) \
                    
display(df1)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Find duplicate and removed

# COMMAND ----------

check_dup = df1.groupBy('circuit_id').count().filter(col('count') > 1)
display(check_dup)
window = Window.partitionBy('circuit_id').orderBy('circuit_id')
df1 = df1.withColumn('rn',row_number().over(window)).filter(col('rn') == 1).drop('rn')
# display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null value 

# COMMAND ----------

check_null = df1.select( *[count(when(col(c).isNull(),1)).alias(c) for c in df1.columns] )
display(check_null)

# COMMAND ----------

# write to datalake
df1.write.mode('overwrite').format('delta').option('path',f'{processed_path}/circuits').saveAsTable("hive_metastore.f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.circuits ; 