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

df = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sales Dataset.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sales Dataset.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemaa = StructType([
    StructField(" 4c0",IntegerType()),
    StructField("Date",DateType()),
    StructField("Gender",StringType()),
    StructField("Age",IntegerType()),
    StructField("Product Category",StringType()),
    StructField("Quantity",IntegerType()),
    StructField("Price per Unit",IntegerType()),
    StructField("Total Amount",IntegerType())

])
df = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sales Dataset.csv",schema=schemaa)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.toDF(*[col.replace(" ", "_") for col in df.columns])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum, col

# Group by Product_Category and calculate total sales
seg = df.groupBy("Product_Category").agg(sum("Total_Amount").alias("Total_sales"))

# Display the result
seg.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wind = Window.orderBy(col("Total_sales").desc())
rn = seg.withColumn("rank", dense_rank().over(wind) )
display(rn)

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
