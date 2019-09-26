# Databricks notebook source
# MAGIC %sh
# MAGIC wget -O /dbfs/mnt/books.xml https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

with open('/dbfs/mnt/books.xml', 'r') as content_file:
  content = content_file.read()
print(content)

# COMMAND ----------

df = spark.read.format('xml').options(rowTag='book',inferSchema='true').load('/mnt/books.xml')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
customSchema = StructType([ \
    StructField("_id", StringType(), True), \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", DateType(), True), \
    StructField("title", StringType(), True)])

# COMMAND ----------

dfWithSchema = spark.read.format('xml').options(rowTag='book').load('/mnt/books.xml',schema = customSchema)


# COMMAND ----------

dfWithSchema.printSchema()

# COMMAND ----------

display(dfWithSchema)