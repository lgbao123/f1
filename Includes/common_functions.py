# Databricks notebook source
def partitionOverwrite(dbname,tablename,df,parttion_column):
    try :
        if spark._jsparkSession.catalog().tableExists(dbname,tablename):
            for row in df.select(parttion_column).distinct().collect():
                spark.sql(f'alter table {dbname}.{tablename} drop if exists partition ({parttion_column} = {row[parttion_column]})')
        df.write.mode('append').partitionBy(parttion_column).format('parquet').saveAsTable(f'{dbname}.{tablename}')
        print(f'Write success to {dbname}.{tablename}')
    except Exception as e:
        print(e)

# COMMAND ----------

def changeDistinctColumnToList(df , column):
    return [row[column] for row in df.select(column).distinct().collect()]

# COMMAND ----------


