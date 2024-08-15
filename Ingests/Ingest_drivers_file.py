# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %run ../Includes/common_functions

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
# MAGIC #### Read file

# COMMAND ----------

relative_path = f'/ErgastApi/Driver/year={year}/month={month}/day={day}/driver.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convention name, cast value, add audit column into data

# COMMAND ----------

# transform
drivers_df = df\
                .select('MRData.DriverTable.Drivers') \
                .withColumn('Drivers',explode('Drivers')) \
                .select(
                    col('Drivers.driverId').alias('driver_id'),
                    col('Drivers.familyName').alias('family_name'),
                    col('Drivers.givenName').alias('given_name'),
                    col('Drivers.dateOfBirth').cast('date').alias('date_of_birth'),
                    col('Drivers.nationality').alias('driver_nationality'),
                    col('Drivers.permanentNumber').cast('int').alias('driver_number'),
                ) \
                .withColumn('ingestion_date',lit(p_file_date).cast('date'))
display(drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Find and remove duplicate

# COMMAND ----------

check_dup =  drivers_df.groupBy('driver_id').count().filter(col('count') > 1)
display(check_dup)
window= Window.partitionBy('driver_id').orderBy('given_name')
drivers_df =drivers_df.withColumn('rn',row_number().over(window)).filter(col('rn')==1).drop('rn')
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null

# COMMAND ----------

checkNullDF(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write df to datalake 

# COMMAND ----------

# write to datalake
drivers_df.write.mode('overwrite').format('delta').option('path',f'{processed_path}/drivers').saveAsTable('hive_metastore.f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from hive_metastore.f1_processed.drivers