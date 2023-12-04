# Databricks notebook source
p_file_date = '2021-03-28'
p1 =dbutils.notebook.run('Ingest_circuits_file',0,{'p_file_date':p_file_date})
p2 =dbutils.notebook.run('Ingest_races_file',0,{'p_file_date':p_file_date})
p3 =dbutils.notebook.run('Ingest_constructors_file',0,{'p_file_date':p_file_date})
p4 =dbutils.notebook.run('Ingest_drivers_file',0,{'p_file_date':p_file_date})
p5 =dbutils.notebook.run('Ingest_results_file',0,{'p_file_date':p_file_date})
p6 =dbutils.notebook.run('Ingest_pitstops_file',0,{'p_file_date':p_file_date})
p7 =dbutils.notebook.run('Ingest_lap_times_file',0,{'p_file_date':p_file_date})
p8 =dbutils.notebook.run('Ingest_qualifying_file',0,{'p_file_date':p_file_date})
[p1,p2,p3,p4,p5,p6,p7,p8]

# COMMAND ----------

p_file_date = '2021-03-28'
p1 =dbutils.notebook.run('../trans/race_results',0,{'p_file_date':p_file_date})
p2 =dbutils.notebook.run('../trans/driver_standings',0,{'p_file_date':p_file_date})
p3 =dbutils.notebook.run('../trans/race_results',0,{'p_file_date':p_file_date})
[p1,p2,p3]

# COMMAND ----------


