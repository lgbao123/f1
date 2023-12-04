# Databricks notebook source
# MAGIC %run ../Includes/configuration

# COMMAND ----------

def mountADLS(storage_account_name , container_name ,access_token):
    storageAccountName = storage_account_name
    storageAccountAccessKey = access_token
    blobContainerName = container_name
    mountPoint = f"/mnt/{storage_account_name}/{container_name}/"
    if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mountPoint)
    try:
        dbutils.fs.mount(
        source = f"wasbs://{blobContainerName}@{storageAccountName}.blob.core.windows.net",
        mount_point = mountPoint,
        extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
        )
        print(f"mount {container_name} succeeded!")
    except Exception as e:
        print("mount exception", e)

# COMMAND ----------

mountADLS(acc_name,'raw',acc_access_token)
mountADLS(acc_name,'processed',acc_access_token)
mountADLS(acc_name,'presentation',acc_access_token)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


