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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerData").getOrCreate()

# Sample data
data = [
    (1, "Alice", "9876543210"),
    (2, "Bob", "9123456789"),
    (3, "Charlie", "9988776655"),
    (4, "David", "9090909090"),
    (5, "Eva", "8008008008"),
    (6, "Frank", "98A76B3210"),
    (7, "Grace", "91234X6789"),
    (8, "Hank", "9988@76655"),
    (9, "Ivy", "90#0909090"),
    (10, "Jack", "8008O08008")
]

# Define schema
schema = StructType([
    StructField("custid", IntegerType(), True),
    StructField("custname", StringType(), True),
    StructField("cust_phonenumber", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, regexp_extract

df.withColumn(
    "newnumber",
    regexp_extract(col("cust_phonenumber"), "^\d{10}$", 0)
).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("manager_id", IntegerType(), True)
])


data = [
    (1, "Alice", 50000, "HR", date(1990, 5, 12), 4),
    (2, "Bob", 60000, "IT", date(1988, 8, 23), 5),
    (3, "Charlie", 55000, "Finance", date(1991, 1, 5), 2),
    (4, "David", 90000, "IT", date(1985, 9, 30), 3),
    (5, "Eve", 52000, "HR", date(1992, 3, 14), None),
    (6, "Frank", 75000, "Finance", date(1987, 12, 18), 4),
    (7, "Grace", 81000, "IT", date(1993, 7, 1), 8),
    (8, "Hank", 48000, "Marketing", date(1990, 10, 10), None),
    (9, "Ivy", 59000, "HR", date(1989, 6, 27), 8),
    (10, "Jack", 61000, "Marketing", date(1991, 11, 3), None)
]
gh = spark.createDataFrame(data, schema=schema)
display(gh)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

a = gh.alias("a")
b = gh.alias("b")

result = a.join(b, on=col("a.manager_id") == col("b.id"), how="left") \
          .select(col("a.*"), col("b.name").alias("manager_name"))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.filter(result.manager_id.isNotNull())
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MismatchExample").getOrCreate()

# First DataFrame
data1 = [
    (1, "Alice", 50000),
    (2, "Bob", 60000),
    (3, "Charlie", 70000)
]
df1 = spark.createDataFrame(data1, ["id", "name", "salary"])

# Second DataFrame
data2 = [
    (1, "Alice", 50000),
    (2, "Bob", 65000),       # Salary mismatch
    (4, "David", 70000)      # ID mismatch
]

df2 = spark.createDataFrame(data2, ["id", "name", "salary"])
display(df1)
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_mismatch(df1, df2, join_column):
    joined_df = df1.alias("df1").join(df2.alias("df2"), on=join_column, how="outer")
    mismatch_df = joined_df.filter(
        (col("df1.name") != col("df2.name")) |
        (col("df1.salary") != col("df2.salary")) |
        col("df1.id").isNull() |
        col("df2.id").isNull()
    )
    return mismatch_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mismatches = find_mismatch(df1, df2, "id")
mismatches.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName("ExtractAfterDigits").getOrCreate()

# Sample data
data = [("ASKBHGJ45JGHG",), ("XYZ123ABC",), ("NO1234DATA",)]
df = spark.createDataFrame(data, ["input_string"])

# Regular expression to extract characters after the last digit
# This pattern finds the last sequence of digits and captures everything after it
df = df.withColumn("after_digits", regexp_extract("input_string", r"\d+(.*)", 1))

df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data=[('ABSHFJFJ12QWERT12',1),('QWERT5674OTUT1',2),('DGDGNJDJ1234UYI',3)]
df=spark.createDataFrame(data,schema="input_string string,id int")
df.show()

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

df.withColumn("newcol",regexp_extract(col("input_string"),r"\d+(.*)",1)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


data=[(1,"Sagar-Prajapati"),(2,"Alex-John"),(3,"John Cena"),(4,"Kim Joe")]
schema="ID int,Name string"
dff=spark.createDataFrame(data,schema)
display(dff)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dff.withColumn("new",regexp_replace(col("Name"),"-",' ')).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Create Spark session
spark = SparkSession.builder.appName("GenerateDataset").getOrCreate()

# Create base data
data = [(i, f"Name{i}") for i in range(1, 26)]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name"])

# Add description column with every 5th row same as previous
dfg = df.withColumn(
    "description",
    when((col("id") % 5 == 0), lit("Same as previous")).otherwise(col("name") + "_desc")
)

dfg.show(30, truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create Spark session
spark = SparkSession.builder.appName("RandomDescriptions").getOrCreate()

# Function to generate random description
def generate_random_description():
    words = ["dude", "awesome", "spark", "data", "python", "cool", "fast", "big", "AI", "machine"]
    return " ".join(random.choices(words, k=3))

# Create data with random descriptions
data = [(i, f"Name{i}", generate_random_description()) for i in range(1, 26)]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

df.show(30, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter((col("id") % 2 == 0) & (~col("description").contains("AI"))).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [(1, "abc@gmail.com"), (2, "bcd@gmail.com"), (3, "abc@gmail.com")]
schema = "ID int,email string"
df = spark.createDataFrame(data, schema)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df.groupBy(col("email")).count().filter(col("count")==2).show()

from pyspark.sql.window import *
y =  Window.partitionBy("email").orderBy(col("ID").desc())
f = df.withColumn("rn",row_number().over(y))
f.filter(col("rn")==2).show()





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data=[(1,'Sagar'),(2,'Alex'),(3,'John'),(4,'Kim')]
schema="Customer_ID int, Customer_Name string"
df_customer=spark.createDataFrame(data,schema)

data=[(1,4),(3,2)]
schema="Order_ID int, Customer_ID int"
df_order=spark.createDataFrame(data,schema)
display(df_customer)
display(df_order)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customer.join(df_order, on='Customer_ID', how='left_anti').show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data=[('Genece' , 2 , 75000),
('𝗝𝗮𝗶𝗺𝗶𝗻' , 2 , 80000 ),
('𝗣𝗮𝗻𝗸𝗮𝗷' , 2 , 80000 ),
('Tarvares' , 2 , 70000),
('Marlania' , 4 , 70000),
('Briana' , 4 , 85000),
('𝗞𝗶𝗺𝗯𝗲𝗿𝗹𝗶' , 4 , 55000),
('𝗚𝗮𝗯𝗿𝗶𝗲𝗹𝗹𝗮' , 4 , 55000),  
('Lakken', 5, 60000),
('Latoynia' , 5 , 65000) ]
schema="emp_name string,dept_id int,salary int"
df=spark.createDataFrame(data,schema)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max, min

df.groupBy("dept_id").agg(
    max(col("salary")).alias("mx_sal"),
    min(col("salary")).alias("min_sal")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

# Create Spark session
spark = SparkSession.builder.appName("NullPercentage").getOrCreate()

# Sample data with null values
data = [
    (None, "A", "X", None, "P"),
    ("B", None, "Y", None, "Q"),
    ("C", "D", None, None, "R"),
    (None, "E", "Z", None, "S"),
    ("F", "G", None, None, "T"),
    ("H", "I", "W", None, "U"),
    ("J", "K", "V", None, "V"),
    ("L", "M", "U", "N", "W"),
    ("M", "N", "T", "O", "X"),
    ("N", "O", "S", "P", "Y")
]

columns = ["col1", "col2", "col3", "col4", "col5"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Total number of rows
total_rows = df.count()

# Calculate null percentages
null_percentages = df.select([
    (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c + "_null_pct")
    for c in df.columns
])

null_percentages.show()


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

nul = df.select([
    (count(when(col(c).isNull(), c)).alias("Null_count")) for c in df.columns])

display(nul)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Create Spark session (only needed if you're running this outside a notebook)
spark = SparkSession.builder.appName("ExtractJSON").getOrCreate()

# Sample data
data = [
    ('John Doe', '{"street": "123 Main St", "city": "Anytown"}'),
    ('Jane Smith', '{"street": "456 Elm St", "city": "Othertown"}')
]

# Create initial DataFrame
df = spark.createDataFrame(data, schema="name string, address string")

# Define schema for JSON parsing
address_schema = StructType() \
    .add("street", StringType()) \
    .add("city", StringType())

# Parse JSON and extract fields
df_parsed = df.withColumn("address_json", from_json(col("address"), address_schema))
df_final = df_parsed.select(
    col("name"),
    col("address_json.street").alias("street"),
    col("address_json.city").alias("city")
)

# Display the final DataFrame
display(df_final)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

address_schema = StructType() \
    .add("street", StringType()) \
    .add("city", StringType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
 (1, ),
 (2,),
 (3,),
 (6,),
 (7,),
 (8,)]
schema="Id int"
df = spark.createDataFrame(data,schema=schema)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_list = df.select(min(col("Id")).alias("min"), max(col("Id")).alias("maxid"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new = spark.range(df_list.first()[0],df_list.first()[1]+1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(df_new)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
