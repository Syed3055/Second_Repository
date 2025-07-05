# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0963a2d1-7145-408a-a3da-06cf3f8dea0f",
# META       "default_lakehouse_name": "Lakehouse_01",
# META       "default_lakehouse_workspace_id": "2baf3d08-a1ba-4127-9f42-c2ba3c1217e6",
# META       "known_lakehouses": [
# META         {
# META           "id": "0963a2d1-7145-408a-a3da-06cf3f8dea0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cu = spark.read.format("csv").option("header","true").load("Files/customers (1).csv")
# df now is a Spark DataFrame containing CSV data from "Files/customers (1).csv".
display(cu)

pr = spark.read.format("csv").option("header","true").load("Files/products (1).csv")
# df now is a Spark DataFrame containing CSV data from "Files/products (1).csv".
display(pr)

sa = spark.read.format("csv").option("header","true").load("Files/sales (1).csv")
# df now is a Spark DataFrame containing CSV data from "Files/sales (1).csv".
display(sa)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cu=cu.withColumn("extracted", regexp_extract("Email", "example([^.]*)", 0))
display(cu)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cu = cu.withColumn("cleaned_column", regexp_replace("CustomerName", "[^a-zA-Z0-9]", ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cu)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pr = pr.withColumn("cleaned_pr",regexp_replace("ProductName","[^a-zA-Z0-9]",""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(pr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cu)
display(pr)
display(sa)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cs = sa.join(cu,on="CustomerID",how="inner").select(cu["*"],"Amount","ProductID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fj = pr.join(cs,on="ProductID",how="inner").select(cs["*"],"ProductName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fj)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tt = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/random_data.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/random_data.csv".
display(tt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Filter rows where string_col2 is null and show them
g = tt.select(col("string_col2").isNull()).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tt = tt.withColumn("int_col",col("int_col").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tt.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(tt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tt.filter(col("string_col2")=='JFFTJ').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
