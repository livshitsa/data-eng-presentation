# Databricks notebook source
# MAGIC %run ./sfcredentials

# COMMAND ----------

options ={
  "username":sf_username,
  "password":sf_password,
  "login":sf_login,
  "version": "44.0",
  "soql":""
}

# COMMAND ----------

soqlQuery="SELECT AccountNumber,Id,Name,ShippingAddress,ShippingCity,ShippingCountry,ShippingPostalCode,ShippingState,ShippingStreet FROM Account"

# COMMAND ----------

options.update({"soql":soqlQuery})
dfSF = spark.read.format("com.springml.spark.salesforce").options(**options).load()

# COMMAND ----------

display(dfSF)