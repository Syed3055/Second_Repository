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

df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sc = StructType([
    StructField("Row ID",IntegerType()),
    StructField("Order ID",StringType()),
    StructField("Order Date",StringType()),
    StructField("Ship Date",StringType()),
    StructField("Ship Mode",StringType()),
    StructField("Customer ID",StringType()),
    StructField("Customer Name",StringType()),
    StructField("Segment",StringType()),
    StructField("City",StringType()),
    StructField("State",StringType()),
    StructField("Country",StringType()),
    StructField("Region",StringType()),
    StructField("Product ID",StringType()),
    StructField("Category",StringType()),
    StructField("Sub-Category",StringType()),
    StructField("Product Name",StringType()),
    StructField("Sales",FloatType()),
    StructField("Qunatity",IntegerType()),
    StructField("Discount",DoubleType()),
    StructField("Profit",FloatType())
    ])







df = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sample - EU Superstore.csv", schema=sc)
# df now is a Spark DataFrame containing CSV data from "abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sample - EU Superstore.csv".
display(df)
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.toDF(*[col.replace(" ","_") for col in df.columns])

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

cs = df.groupby("Category").agg(sum(col("Sales")).alias("TotalSales"))

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

df= df.withColumn("Code",split(col("Order_ID"),"-")[0])\
.withColumn("C_Year",split(col("Order_ID"),"-")[1])\
.withColumn("TicketNo",split(col("Order_ID"),"-")[2])
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wind = Window.partitionBy(col("Category")).orderBy(col("Totalsales").desc())

gp=gp.withColumn("rank",dense_rank().over(wind))
display(gp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("parquet").mode("overwrite").partitionBy("Category").save("Tables/Partition_Table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select("Ship_Mode","Order_ID"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dd = spark.read.parquet("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Partition_Table")
display(dd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dd.filter(col("Category")=="Furniture"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(col("Category")=="Furniture"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy("Category","Sub-Category").agg(sum(col("Sales"))).show()

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
