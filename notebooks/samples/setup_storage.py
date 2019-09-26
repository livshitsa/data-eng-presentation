# Databricks notebook source
storage_account_name = 'your storage account name'
storage_account_key = dbutils.secrets.get(scope = 'data-eng', key = 'storagekey')
mount_source_base=storage_account_name+".blob.core.windows.net"
mount_extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net": storage_account_key}

# COMMAND ----------

if (not list(filter(lambda x: "/mnt/dataeng/test" in x.mountPoint, dbutils.fs.mounts()))):
  dbutils.fs.mount(source = "wasbs://test@"+mount_source_base,
                   mount_point = "/mnt/dataeng/test",
                   extra_configs = mount_extra_configs)

# COMMAND ----------

if (not list(filter(lambda x: "/mnt/dataeng/results" in x.mountPoint, dbutils.fs.mounts()))):
  dbutils.fs.mount(source = "wasbs://results@"+mount_source_base,
                   mount_point = "/mnt/dataeng/results",
                   extra_configs = mount_extra_configs)

# COMMAND ----------

if (not list(filter(lambda x: "/mnt/dataeng/zone" in x.mountPoint, dbutils.fs.mounts()))):
  dbutils.fs.mount(source = "wasbs://zone@"+mount_source_base,
                   mount_point = "/mnt/dataeng/zone",
                   extra_configs = mount_extra_configs)
