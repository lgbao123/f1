# Databricks notebook source
if (spark.catalog.databaseExists("hive_metastore.f1_processed")==False):
    spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.f1_processed ;')
if (spark.catalog.databaseExists("hive_metastore.f1_presentation")==False):
    spark.sql('CREATE DATABASE IF NOT EXISTS hive_metastore.f1_presentation ;')

p_file_date = dbutils.widgets.get('ingestion_date')
dbutils.notebook.run('Ingest_circuits_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('Ingest_races_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('Ingest_constructors_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('Ingest_drivers_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('Ingest_results_file',0,{'p_file_date':p_file_date})
dbutils.notebook.run('Ingest_qualifying_file',0,{'p_file_date':p_file_date})