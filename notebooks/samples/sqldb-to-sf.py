# Databricks notebook source
server = "db-srv.database.windows.net"
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

# query="select top 1 \
#     cust.customerid, \
#     cust.companyname, \
#     custaddr.addresstype, \
#     addr.countryregion, \
#     addr.addressline1, \
#     addr.city, \
#     addr.stateprovince ,\
# 	addr.postalcode \
# from saleslt.customer as cust \
# join saleslt.customeraddress as custaddr on custaddr.customerid = cust.customerid \
# join saleslt.address as addr on addr.addressid = custaddr.addressid \
# where addr.countryregion = 'United States'"

# COMMAND ----------

query="select top 5 \
    cust.customerid, \
    cust.companyname, \
    addr.city, \
    addr.stateprovince, \
    addr.postalcode \
from saleslt.customer as cust \
join saleslt.customeraddress as custaddr on custaddr.customerid = cust.customerid \
join saleslt.address as addr on addr.addressid = custaddr.addressid \
where addr.countryregion = 'United States'"

# COMMAND ----------

import copy
curSqlOptions = copy.deepcopy(sqldboptions)
curSqlOptions.update({"query":query} )


# COMMAND ----------

dfSQLCustomers = spark.read.format("jdbc").options(**curSqlOptions).load()

# COMMAND ----------

display(dfSQLCustomers)

# COMMAND ----------

dfCustomers = dfSQLCustomers.withColumnRenamed("customerid","account_number__c")\
.withColumnRenamed("companyname","Name")\
.withColumnRenamed("city","ShippingCity")\
.withColumnRenamed("stateprovince","ShippingState")\
.withColumnRenamed("postalcode","ShippingPostalCode")

# COMMAND ----------

display(dfCustomers)

# COMMAND ----------

# MAGIC %run ./sfcredentials

# COMMAND ----------

sfoptions ={
  "username":sf_username,
  "password":sf_password,
  "login":sf_login,
  "version": "44.0",
  "sfObject":"Account",
  "externalIdFieldName":"account_number__c",
  "upsert": "true"
}

# COMMAND ----------

dfCustomers.write.format("com.springml.spark.salesforce").options(**sfoptions).save()