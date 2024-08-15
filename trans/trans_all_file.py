# Databricks notebook source
p_file_date = dbutils.widgets.get('ingestion_date')
# p1 =dbutils.notebook.run('race_results',0,{'p_file_date':p_file_date})
# p2 =dbutils.notebook.run('driver_standings',0,{'p_file_date':p_file_date})
p3 =dbutils.notebook.run('race_results',0,{'p_file_date':p_file_date})