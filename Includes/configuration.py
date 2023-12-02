# Databricks notebook source
acc_name = 'formula12609dl'
acc_access_token = dbutils.secrets.get('f1','f1dl-access-key')
raw_path = 'abfss://raw@formula12609dl.dfs.core.windows.net'
processed_path = 'abfss://processed@formula12609dl.dfs.core.windows.net'
presentation_path = 'abfss://presentation@formula12609dl.dfs.core.windows.net'
