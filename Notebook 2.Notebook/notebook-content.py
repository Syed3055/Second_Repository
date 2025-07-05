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


# METADATA *******************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sample - EU Superstore.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sample - EU Superstore.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemaa = StructType([
    StructField("Row ID",IntegerType() ),
    StructField("Order ID",StringType()),
    StructField("Order Date",StringType()),
    StructField("Ship Date",StringType()),
    StructField("Ship Mode", StringType()),
    StructField("Customer ID",StringType()),
    StructField("Customer Name",StringType()),
    StructField("Segment",StringType()),
    StructField("City",StringType()),
    StructField("State",StringType()),
    StructField("Country",StringType()),
    StructField("Region",StringType()),
    StructField("Product",StringType()),
    StructField("Category",StringType()),
    StructField("Sub-Category",StringType()),
    StructField("Product Name",StringType()),
    StructField("Sales",FloatType()),
    StructField("Quantity",IntegerType()),
    StructField("Profit",FloatType())

    
])
df = spark.read.format("csv").option("header","true").load("abfss://2baf3d08-a1ba-4127-9f42-c2ba3c1217e6@onelake.dfs.fabric.microsoft.com/0963a2d1-7145-408a-a3da-06cf3f8dea0f/Files/Sample - EU Superstore.csv",schema=schemaa)

display(df)


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

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace spaces with underscores in all column names
df = df.toDF(*[col.replace(" ", "_") for col in df.columns])


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

df2 = df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

minn = df.groupBy("Customer ID").agg(min(col("Order Date")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("sample")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final = spark.sql("""
WITH cte AS (
    SELECT Customer_ID, MIN(Order_Date) AS first_order
    FROM sample
    GROUP BY Customer_ID
)
SELECT 
    s.Order_Date,
   
    COUNT(CASE WHEN s.Order_Date = cte.first_order THEN 1 END) AS new_customer,
    COUNT(CASE WHEN s.Order_Date != cte.first_order THEN 1 END) AS repeated_customer
FROM sample s
JOIN cte ON s.Customer_ID = cte.Customer_ID
GROUP BY s.Order_Date
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final.filter(col("Order_Date")=='05/03/2017').distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(col("Order_Date")=='05/03/2017').distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df.groupBy("Category", "Sub-Category").agg(sum("Sales").alias("total_sales"), sum("Profit").alias("Total_Procit")).show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shipmode = df.groupby("Ship_Mode").agg(count("*").alias("Total_Orders"), round(sum("Sales"),2).alias("Total_sales ($)"),round(sum("Profit"),2).alias("Total_Profit ($)")).orderBy("Total_Orders",ascending=False) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(shipmode)

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

display(df.select("Ship_Mode").distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def status(value):
    if value=='Standard Class':
        return "Standard"
    elif value=='Second Class':
        return "Second"
    elif value=="Same Day":
        return "Same"
        
    else:
        return "First"
udff = udf(status, StringType())
df = df.withColumn("Shipping_Status", udff(df.Ship_Mode))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df= df.withColumn("Code",split(col("Customer_ID"),"-")[0])
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table syed(
# MAGIC     id int,
# MAGIC     name varchar(10)
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("Ship_Mode","Shipping_Status").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(shipmode)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

win = Window.orderBy(col("Total_sales ($)").desc())
t = shipmode.withColumn("rank",rank().over(win))
display(t)

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
