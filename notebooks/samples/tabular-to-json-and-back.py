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

query="""
select 
    soh.salesorderid,
    sod.orderqty,
    sod.productid,
    prod.name, 
    sod.unitprice
from saleslt.salesorderdetail as sod 
join saleslt.salesorderheader as soh on soh.salesorderid = sod.salesorderid 
join saleslt.product as prod on sod.productid = prod.productid
"""

# COMMAND ----------

sqldboptions.update({"query":query} )

# COMMAND ----------

resultDF = spark.read.format("jdbc").options(**sqldboptions).load()

# COMMAND ----------

display(resultDF)

# COMMAND ----------

from pyspark.sql import functions as F
nestedDF = resultDF.groupBy('salesorderid').\
  agg(\
    F.collect_list(
      F.struct(F.col('name').alias('product_name'),\
      F.col('unitprice').alias('unit_price'),\
      F.col('orderqty').alias('order_quantity')\
      )\
  ).alias('order_items')
)

# COMMAND ----------

display(nestedDF)

# COMMAND ----------

nestedDF.coalesce(1).write.mode("overwrite").format('json').save('/mnt/dataeng/results/nested')

# COMMAND ----------

nestedfiles = dbutils.fs.ls('/mnt/dataeng/results/nested')
output_jsonfiles = [x for x in nestedfiles if x.name.startswith("part-")]

dbutils.fs.mv(output_jsonfiles[0].path, '/mnt/dataeng/results/orders.json')
dbutils.fs.rm('/mnt/dataeng/results/nested',True)

# COMMAND ----------

jsonDF = spark.read.json('/mnt/dataeng/results/orders.json')

# COMMAND ----------

jsonDF.printSchema()

# COMMAND ----------

display(jsonDF)

# COMMAND ----------

from pyspark.sql.functions import explode,get_json_object
denormalizedDF = jsonDF.select('salesorderid',explode('order_items').alias('order_item'))
denormalizedDF = denormalizedDF.select('salesorderid','order_item.order_quantity','order_item.product_name','order_item.unit_price')

# COMMAND ----------

display(denormalizedDF)