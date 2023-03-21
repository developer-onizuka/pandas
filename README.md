# pandas
pandas is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool, built on top of the Python programming language.

# 1. Python for Jupyther notebook associated with Spark
```
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductNumber", StringType()),
    StructField("ProductName", StringType()),
    StructField("ModelName", StringType()),
    StructField("MakeFlag", IntegerType()),
    StructField("StandardCost", StringType()),
    StructField("ListPrice", FloatType()),
    StructField("SubCategoryID", IntegerType())
    ])

df = spark.read.option('header','true') \
               .option('delimiter',',') \
               .schema(productSchema) \
               .csv('products.csv')
df = df.withColumn("ListPrice", df["ListPrice"]+10)
df.printSchema()
df.show()
```
```
+---------+-------------+--------------------+--------------------+--------+------------+---------+-------------+
|ProductID|ProductNumber|         ProductName|           ModelName|MakeFlag|StandardCost|ListPrice|SubCategoryID|
+---------+-------------+--------------------+--------------------+--------+------------+---------+-------------+
|      680|   FR-R92B-58|HL Road Frame - B...|       HL Road Frame|       1|     1059.31|   1441.5|           14|
|      706|   FR-R92R-58|HL Road Frame - R...|       HL Road Frame|       1|     1059.31|   1441.5|           14|
|      707|    HL-U509-R|Sport-100 Helmet,...|           Sport-100|       0|     13.0863|    44.99|           31|
|      708|      HL-U509|Sport-100 Helmet,...|           Sport-100|       0|     13.0863|    44.99|           31|
|      709|    SO-B909-M|Mountain Bike Soc...| Mountain Bike Socks|       0|      3.3963|     19.5|           23|
|      710|    SO-B909-L|Mountain Bike Soc...| Mountain Bike Socks|       0|      3.3963|     19.5|           23|
|      711|    HL-U509-B|Sport-100 Helmet,...|           Sport-100|       0|     13.0863|    44.99|           31|
|      712|      CA-1098|        AWC Logo Cap|         Cycling Cap|       0|      6.9223|    18.99|           19|
|      713|    LJ-0192-S|Long-Sleeve Logo ...|Long-Sleeve Logo ...|       0|     38.4923|    59.99|           21|
|      714|    LJ-0192-M|Long-Sleeve Logo ...|Long-Sleeve Logo ...|       0|     38.4923|    59.99|           21|
|      715|    LJ-0192-L|Long-Sleeve Logo ...|Long-Sleeve Logo ...|       0|     38.4923|    59.99|           21|
|      716|    LJ-0192-X|Long-Sleeve Logo ...|Long-Sleeve Logo ...|       0|     38.4923|    59.99|           21|
|      717|   FR-R92R-62|HL Road Frame - R...|       HL Road Frame|       1|    868.6342|   1441.5|           14|
|      718|   FR-R92R-44|HL Road Frame - R...|       HL Road Frame|       1|    868.6342|   1441.5|           14|
|      719|   FR-R92R-48|HL Road Frame - R...|       HL Road Frame|       1|    868.6342|   1441.5|           14|
|      720|   FR-R92R-52|HL Road Frame - R...|       HL Road Frame|       1|    868.6342|   1441.5|           14|
|      721|   FR-R92R-56|HL Road Frame - R...|       HL Road Frame|       1|    868.6342|   1441.5|           14|
|      722|   FR-R38B-58|LL Road Frame - B...|       LL Road Frame|       1|    204.6251|   347.22|           14|
|      723|   FR-R38B-60|LL Road Frame - B...|       LL Road Frame|       1|    204.6251|   347.22|           14|
|      724|   FR-R38B-62|LL Road Frame - B...|       LL Road Frame|       1|    204.6251|   347.22|           14|
+---------+-------------+--------------------+--------------------+--------+------------+---------+-------------+
only showing top 20 rows
```
# DataFrame.createOrReplaceTempView
Creates or replaces a local temporary view with this DataFrame.<br>
The lifetime of this temporary table is tied to the SparkSession that was used to create this DataFrame.
```
df.createOrReplaceTempView("products")
```
# DataFrame.toPandas
Returns the contents of this DataFrame as Pandas pandas.DataFrame.<br>
This is only available if Pandas is installed and available.
```
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
temp = spark.sql("SELECT ModelName, COUNT(ProductID) AS ProductCount FROM products \
                  GROUP BY ModelName \
                  ORDER BY ModelName")

temp.createOrReplaceTempView("productcount")
data = spark.sql("SELECT ModelName, ProductCount FROM productcount \
                  WHERE ProductCount > 1").toPandas()
```
```
# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by ModelName
plt.bar(x=data['ModelName'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by ModelName grater than one')
plt.xlabel('ModelName')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
```
The Matplotlib library requires data to be in a Pandas dataframe rather than a Spark dataframe, so the **toPandas method** is used to convert it. The code then creates a figure with a specified size and plots a bar chart with some custom property configuration before showing the resulting plot.

The chart produced by the code would look similar to the following image:<br>
![pandas.png](https://github.com/developer-onizuka/pandas/blob/main/pandas.png)


# 2. Scala for Jupyther notebook associated with Spark
```
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val productSchema = StructType(Array(
    StructField("ProductID", IntegerType),
    StructField("ProductNumber", StringType),
    StructField("ProductName", StringType),
    StructField("ModelName", StringType),
    StructField("MakeFlag", IntegerType),
    StructField("StandardCost", StringType),
    StructField("ListPrice", FloatType),
    StructField("SubCategoryID", IntegerType)))

val df = spark.read.format("csv").option("header","true").schema(productSchema).load("products.csv")
df.withColumn("ListPrice", df("ListPrice")+10)
df.printSchema()
df.show()
```
# %%python script magic
Run cells with python in a subprocess.
```
%%python
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
temp = spark.sql("SELECT ModelName, COUNT(ProductID) AS ProductCount FROM products \
                  GROUP BY ModelName \
                  ORDER BY ModelName")

temp.createOrReplaceTempView("productcount")
data = spark.sql("SELECT ModelName, ProductCount FROM productcount \
                  WHERE ProductCount > 1").toPandas()
```
```
%%python

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))
```
```
%matplotlib notebook
%%python

# Create a bar plot of product counts by ModelName
plt.bar(x=data['ModelName'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by ModelName grater than one')
plt.xlabel('ModelName')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
```
