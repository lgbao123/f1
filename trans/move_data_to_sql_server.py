# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #### read data from delta table

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
results_df = spark.read.format('delta').load(f'{presentation_path}/race_results').orderBy(col('season').desc())
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write

# COMMAND ----------

# write to datalake
server_name = dbutils.widgets.get('server_name')
sqldbname = dbutils.widgets.get('sqldbname')
table = dbutils.widgets.get('table')
user = dbutils.widgets.get('user')
password = dbutils.secrets.get('azure-scope', 'sqlserverPassword')

results_df.write.format("sqlserver") \
  .mode("overwrite") \
  .option("host", server_name) \
  .option("port", 1433) \
  .option("database", sqldbname) \
  .option("dbtable", table) \
  .option("user", user) \
  .option("password", password) \
  .save()

# COMMAND ----------

dbutils.notebook.exit('success')