# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

processed_path

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS hive_metastore.f1_processed CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.f1_processed 
# MAGIC -- LOCATION 'abfss://fo1@2609adlg2.dfs.core.windows.net/processed';
# MAGIC
# MAGIC DROP DATABASE IF EXISTS hive_metastore.f1_presentation CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.f1_presentation;
# MAGIC --LOCATION 'abfss://fo1@2609adlg2.dfs.core.windows.net/presentation';
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC show databases

# COMMAND ----------

