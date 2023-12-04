# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed 
# MAGIC LOCATION '/mnt/formula12609dl/processed';
# MAGIC
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION '/mnt/formula12609dl/presentation';
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC show databases

# COMMAND ----------


