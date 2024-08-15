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
dbutils.widgets.text('p_file_date','')
p_file_date = dbutils.widgets.get('p_file_date')
year = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%Y')
month = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%m')
day = datetime.strptime(p_file_date,'%Y-%m-%d').strftime('%d')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data

# COMMAND ----------

relative_path = f'/ErgastApi/Constructor/year={year}/month={month}/day={day}/constructor.json'
input_path = f'{raw_path}/{relative_path}'
df = spark.read.json(input_path)
                    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convention name, cast data type, add audit col

# COMMAND ----------

# transform
constructors_df = df\
                    .select('MRData.ConstructorTable.Constructors')\
                    .withColumn('Constructors', explode('Constructors')) \
                    .select(
                        col('Constructors.constructorId').alias('constructor_id'),
                        col('Constructors.name').alias('constructors_name'),
                        col('Constructors.nationality').alias('constructors_nationality'),
                    ) \
                    .withColumn('ingestion_date',lit(p_file_date).cast('date'))\



# COMMAND ----------

display(constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Find and remove duplicate

# COMMAND ----------

check_dup =  constructors_df.groupBy('constructor_id').count().filter(col('count') > 1)
display(check_dup)
window= Window.partitionBy('constructor_id').orderBy('constructors_name')
constructors_df =constructors_df.withColumn('rn',row_number().over(window)).filter(col('rn')==1).drop('rn')
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check null

# COMMAND ----------


checkNullDF(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data

# COMMAND ----------

# write to datalake
constructors_df.write.mode('overwrite').format('delta').option('path',f'{processed_path}/constructors').saveAsTable('hive_metastore.f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.constructors

# COMMAND ----------

