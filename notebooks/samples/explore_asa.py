# Databricks notebook source
planeDF = spark.read.format("csv").option("header", "true")\
  .option("inferSchema", "true").load("/databricks-datasets/asa/planes/plane-data.csv")

# COMMAND ----------

planeDF.count()

# COMMAND ----------

display(planeDF)

# COMMAND ----------

planeDF.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW temp_planes
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/databricks-datasets/asa/planes/plane-data.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_planes

# COMMAND ----------

cleanPlaneDF = planeDF.na.drop(thresh=2)

# COMMAND ----------

display(cleanPlaneDF)

# COMMAND ----------

cleanPlaneDF.count()

# COMMAND ----------

airlinesDF = spark.read.format("csv").option("header", "true")\
  .option("inferSchema", "true").load("/databricks-datasets/asa/airlines/2008.csv")

# COMMAND ----------

display(airlinesDF)

# COMMAND ----------

airlinesDF.count()

# COMMAND ----------

res = airlinesDF.distinct()
res.count()

# COMMAND ----------

resultDF = airlinesDF.join(cleanPlaneDF,airlinesDF.TailNum == cleanPlaneDF.tailnum)

# COMMAND ----------

resultDF.count()

# COMMAND ----------

display(resultDF)

# COMMAND ----------

resultDF2 = resultDF.drop("tailnum")

# COMMAND ----------

display(resultDF2)

# COMMAND ----------

# resultDF2.describe().show()

# COMMAND ----------

aggResultDF = resultDF2.groupBy("manufacturer").count()

# COMMAND ----------

display(aggResultDF)

# COMMAND ----------

cleanPlaneDF.write.mode("overwrite").format('delta').save('/mnt/dataeng/test/planes')


# COMMAND ----------

# airlinesDF.write.mode("overwrite").format('delta').save('/mnt/dataeng/test/airlines')

# COMMAND ----------

airlinesDF.coalesce(1).write.mode("overwrite").format('delta').save('/mnt/dataeng/test/airlines')

# COMMAND ----------

deltaAirlinesDf = spark.read.format('delta').load('/mnt/dataeng/test/airlines')


# COMMAND ----------

deltaAirlinesDf.count()

# COMMAND ----------

deltaPlanesDf = spark.read.format('delta').load('/mnt/dataeng/test/planes')

# COMMAND ----------

deltaResultDF = deltaAirlinesDf.join(deltaPlanesDf,deltaAirlinesDf.TailNum == deltaPlanesDf.tailnum)

# COMMAND ----------

deltaResultDF.count()