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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/customers (1).csv")
# df now is a Spark DataFrame containing CSV data from "Files/customers (1).csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").save("Tables/customers")

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

# MAGIC %%sql
# MAGIC select * from sample

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from sample where CustomerID in ('C014','C015')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [("John,Smith", "Canada"), ("Mike,David", "USA")]
df = spark.createDataFrame(data,["Names","Country"])
display(df)

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

df = df.withColumn("new",split(col("Names"),","))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("name2",explode(col("new"))).select("name2","Country")

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

schema = StructType([
    StructField("player", StringType(), True),
    StructField("runs", IntegerType(), True),
    StructField("50s/100s", StringType(), True)
])


data = [("Sachin-IND", 18694, "93/49"), ("Ricky-AUS", 11274, "66/31"),("Lara-WI", 10222, "45/21"),("Rahul-IND", 10355, "95/11"),("Jhonty-SA", 7051, "43/5"),("Hayden-AUS", 8722, "67/19")]
players_df = spark.createDataFrame(data, schema)

data1 = [("IND", "India"), ("AUS", "Australia"), ("WI", "WestIndies"), ("SA", "SouthAfrica")]
countries_df = spark.createDataFrame(data1,["SRT","country"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(players_df)
display(countries_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, split

players_df2 = players_df \
    .withColumn("SRT", split(col("player"), "-").getItem(1)) \
    .withColumn("player2", split(col("player"), "-").getItem(0)) \
    .withColumn("50s", split(col("50s/100s"), "/").getItem(0).cast("int")) \
    .withColumn("100s", split(col("50s/100s"), "/").getItem(1).cast("int")) \
    .withColumn("sum", col("50s") + col("100s"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(players_df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mer = players_df2.join(countries_df,on="SRT",how='left')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(mer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mer.select(col("player2").alias("player_name"),col("country"),col("runs"),col("sum")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CorruptedDataFrame").getOrCreate()

# Sample data with 4 corrupted rows
data = [
    ("John", "E001", "New York"),
    ("Alice", "E002", "Los Angeles"),
    ("Bob", "E003", "Chicago"),
    ("Carol", "E004", "Houston"),
    ("Dave", "E005", "Phoenix"),
    ("Eve", "E006", "Philadelphia"),
    ("Frank", "E007", "San Antonio"),
    ("George", "E008", "San Diego, ExtraText"),  # Corrupted
    ("Helen", "E009", "Dallas,123,Extra"),       # Corrupted
    ("Ian,Extra", "E010", "San Jose")            # Corrupted
]

# Create DataFrame
columns = ["empname", "empid", "address"]
df = spark.createDataFrame(data, columns)

df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "DROPMALFORMER") \
    .load("Files/corrupted_employees.csv")

df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("address",split(col("address"),",")[0]).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


schema = StructType([
    StructField("reqid", IntegerType(), True),
    StructField("pickup_location", StringType(), True)
])


data = [(48, "Airport"), (49, "Office"),(50, "Hospital"),(51, "Airport"),(52, "Hospital"),(53, "Shoppingmall"),(54, "Office"),(55, "Hospital"),(56, "Hospital")]
pickup_df = spark.createDataFrame(data, schema)
display(pickup_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import *
pick = pickup_df.groupBy("pickup_location").count()
maxc = pick.agg(max(col("count")).alias("max_count")).collect()[0]["max_count"]

pick.filter(col("count")==maxc).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    (100, 'IT', 100, '2024-05-12'),
    (200, 'IT', 100, '2024-06-12'),
    (100, 'FIN', 400, '2024-07-12'),
    (300, 'FIN', 500, '2024-07-12'),
    (300, 'FIN', 1543, '2024-07-12'),
    (300, 'FIN', 1500, '2024-07-12')
]


columns = ["empid", "dept", "salary", "date"]
dff = spark.createDataFrame(data, columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dff)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gp  = dff.groupby("empid").count()
gpf = gp.filter(col("count")==1).select(col("empid"))
display(gpf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

me = dff.join(gpf,on="empid",how='inner')
display(me)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE emp_new (employee_id VARCHAR(50) );
# MAGIC INSERT INTO emp_new (employee_id) VALUES ('72657'),('1234'),('Tom'),('8792'),('Sam'),('19998'),('Philip')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Sample data from SQL insert
data = [("72657",), ("1234",), ("Tom",), ("8792",), ("Sam",), ("19998",), ("Philip",)]

# Define schema
columns = ["employee_id"]

# Create DataFrame
emp_df = spark.createDataFrame(data, columns)

emp_df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.withColumn("number",regexp_extract(col("employee_id"),"^[0-9]+$",0)).select(col("number")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [("virat kohli",), ("p v sindhu",)]

columns = ["name"]
df = spark.createDataFrame(data, columns)


data1 = [(1, 'Bob'), (2, 'Alice'), (3, 'Tom')]
data2 = [(1, 'Bob'), (3, 'Tom')]


df1 = spark.createDataFrame(data1, ["id", "name"])
df2 = spark.createDataFrame(data2, ["id", "name"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("cap",initcap(col("name"))).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1)
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.join(df1,on='id',how='left_anti').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [(1, None, 'ab'),
    (2, 10, None),
    (None, None, 'cd')]

columns = ['col1', 'col2', 'col3']
f = spark.createDataFrame(data, columns)
display(f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



data = [(101, 'IT', 1000), (102, 'HR', 900)]
columns = ["empid", "dept", "salary"]
dfg = spark.createDataFrame(data, columns)
display(dfg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

g = ["pre_"+ c for c in dfg.columns]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(g)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df.filter(col("col1").isNull() OR col("col2").isNull() OR col("col3").isNull()).show()


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
