# Databricks notebook source
from delta.tables import DeltaTable

def partitionOverwrite(df,dbname,tablename,parttion_column,path,condition):
    try :
        
        if spark._jsparkSession.catalog().tableExists(dbname,tablename):
            targetDF = DeltaTable.forPath(spark, path)
            (targetDF.alias('tgt')
            .merge(df.alias('up'),condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
            )
        else :
            df.write.mode('overwrite').partitionBy(parttion_column).format('delta').saveAsTable(f'{dbname}.{tablename}')
        print(f'Write success to {dbname}.{tablename}')
    except Exception as e:
        print(e)

# COMMAND ----------

def changeDistinctColumnToList(df , column):
    return [row[column] for row in df.select(column).distinct().collect()]

# COMMAND ----------


