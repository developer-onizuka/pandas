# pandas
```
771,"Mountain-100 Silver, 38",Mountain Bikes,3399.9900
772,"Mountain-100 Silver, 42",Mountain Bikes,3399.9900
773,"Mountain-100 Silver, 44",Mountain Bikes,3399.9900
774,"Road-750 Black, 52",Road Bikes,539.9900
```
```
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

df = spark.read.load('product-data.csv',format='csv',schema=productSchema,header=False)
df.show()
```
```
+---------+--------------------+--------------+---------+
|ProductID|         ProductName|      Category|ListPrice|
+---------+--------------------+--------------+---------+
|      771|Mountain-100 Silv...|Mountain Bikes|  3399.99|
|      772|Mountain-100 Silv...|Mountain Bikes|  3399.99|
|      773|Mountain-100 Silv...|Mountain Bikes|  3399.99|
|      774|  Road-750 Black, 52|    Road Bikes|   539.99|
+---------+--------------------+--------------+---------+
```
```
df.createOrReplaceTempView("products")
```
```
from matplotlib import pyplot as plt

# Get the data as a Pandas dataframe
data = spark.sql("SELECT Category, COUNT(ProductID) AS ProductCount \
                  FROM products \
                  GROUP BY Category \
                  ORDER BY Category").toPandas()
```
```
# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(12,8))

# Create a bar plot of product counts by category
plt.bar(x=data['Category'], height=data['ProductCount'], color='orange')

# Customize the chart
plt.title('Product Counts by Category')
plt.xlabel('Category')
plt.ylabel('Products')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=70)

# Show the plot area
plt.show()
```
