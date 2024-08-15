# Databricks notebook source
# MAGIC %md
# MAGIC #### Connect to datalake use access key

# COMMAND ----------


# acc_name = '2609adlg2'
# acc_access_token='wA2AdqfWsWsA2HDoMgxERc2DEZVB9Bato2QONYcDcT+UNm1QV55Fh4h/A7uo+roGoF7j4J+Um9KX+AStett+kQ=='
# spark.conf.set(
#     f"fs.azure.account.key.{acc_name}.dfs.core.windows.net",
#     acc_access_token)

# raw_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/raw'
# processed_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/processed'
# presentation_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/presentation'



# COMMAND ----------

# MAGIC %md
# MAGIC #### Connect to datalake use service principle

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="azure-scope",key="databricks-token")
application_id = 'eb56af58-aec9-4b60-9eb8-848a56b175b3'
directory_id = '8c78c122-f781-4500-9c2a-f645a76281f5'
acc_name = '2609adlg2'

spark.conf.set(f"fs.azure.account.auth.type.{acc_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{acc_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{acc_name}.dfs.core.windows.net", f"{application_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{acc_name}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{acc_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

raw_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/raw'
processed_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/processed'
presentation_path = f'abfss://fo1@{acc_name}.dfs.core.windows.net/presentation'

