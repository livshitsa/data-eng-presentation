# Databricks notebook source
def count_null_cols(row):
  return len([x for x in row if x == None])

# COMMAND ----------

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import IntegerType

df = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
            
count_empty_columns = udf(count_null_cols, IntegerType())

new_df = df.withColumn("null_count", count_empty_columns(struct([df[x] for x in df.columns])))

new_df.show()