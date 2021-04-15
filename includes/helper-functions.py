# Databricks notebook source
# MAGIC %md ## Various helper functions

# COMMAND ----------

# This is a function to mount a storage container
def mountStorageContainer(storageAccount, storageAccountKey, storageContainer, blobMountPoint):
  try:
    print("Mounting {0} to {1}:".format(storageContainer, blobMountPoint))
    # Unmount the storage container if already mounted
    dbutils.fs.unmount(blobMountPoint)
  except Exception as e:
    # If this errors, safe to assume that the container is not mounted
    print("....Container is not mounted; Attempting mounting now..")
    
  # Mount the storage container
  mountStatus = dbutils.fs.mount(
                  source = "wasbs://{0}@{1}.blob.core.windows.net/".format(storageContainer, storageAccount),
                  mount_point = blobMountPoint,
                  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storageAccount): storageAccountKey})

  print("....Status of mount is: " + str(mountStatus))
  print() # Provide a blank line between mounts

# COMMAND ----------


