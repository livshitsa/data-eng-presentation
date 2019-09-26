# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS mysampledb

# COMMAND ----------

server = "yourdb srv.database.windows.net"
database ="alAdventureWorksDB"

port = "1433"

username = dbutils.secrets.get(scope = "data-eng", key = "adventuredbuser")
password = dbutils.secrets.get(scope = "data-eng", key = "adventuredbpw")

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://" + server + ":" + port + ";database=" + database
sqldboptions = {
  "user": username,
  "password": password,
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url": jdbcUrl,
  "query": ""
}

# COMMAND ----------

customerQuery ="""select  
    cust.customerid, 
    cust.companyname 
from saleslt.customer as cust"""

# COMMAND ----------

sqldboptions.update({"query":customerQuery} )

# COMMAND ----------

customersDF = spark.read.format("jdbc").options(**sqldboptions).load()

# COMMAND ----------

customersDF.write.mode("append").format('delta').options(path = '/mnt/dataeng/zone/customer', mergeSchema = 'true').saveAsTable('mysampledb.customer')

# COMMAND ----------

# custDeltaDF = spark.read.format('delta').load('/mnt/dataeng/zone/customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mysampledb.customer

# COMMAND ----------

customersDF2 = spark.read.format("jdbc").options(**sqldboptions).load()

# COMMAND ----------

customersDF2.createOrReplaceTempView("customer2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer2

# COMMAND ----------

  sqlCmd = """MERGE INTO mysampledb.customer AS tgt USING customer2 AS src ON tgt.customerid = src.customerid 
              WHEN MATCHED THEN UPDATE SET * 
              WHEN NOT MATCHED THEN INSERT *"""

# COMMAND ----------

 spark.sql(sqlCmd)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mysampledb.customer where customerid=1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY mysampledb.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mysampledb.customer VERSION AS OF 0 where customerid=1 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mysampledb.customer TIMESTAMP AS OF "2019-09-18 22:58:00" where customerid=1 