# Databricks notebook source

    
def partitionOverwrite(df,dbname,tablename,parttion_column,path,condition):
    from delta.tables import DeltaTable
    try :
        
        if spark.catalog.tableExists(f"hive_metastore.{dbname}.{tablename}"):
            targetDF = DeltaTable.forPath(spark, path)
            (targetDF.alias('tgt')
            .merge(df.alias('up'),condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
            )
        else :
            print('Creating table ...')
            df.write \
                .mode('overwrite') \
                .partitionBy(parttion_column) \
                .format('delta') \
                .option('path', f'{path}') \
                .saveAsTable(f'hive_metastore.{dbname}.{tablename}')
            ## Change version delta table
            spark.sql(f'''
                ALTER TABLE hive_metastore.{dbname}.{tablename} SET TBLPROPERTIES('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5') '''
            )

            
        print(f'Write success to {dbname}.{tablename}')
    except Exception as e:
        print(e)

# COMMAND ----------

def changeDistinctColumnToList(df , column):
    return [row[column] for row in df.select(column).distinct().collect()]

# COMMAND ----------

def checkNullDF(df):
    final = df.select([count(when( (df[c].isNull()) | (df[c]=='') ,True)).alias(c) for c in df.columns])
    final.show()