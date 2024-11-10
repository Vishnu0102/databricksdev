# Databricks notebook source


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("ExampleApp") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Create sample data
data = [
    ("Alice", 28, "New York"),
    ("Bob", 35, "San Francisco"),
    ("Cathy", 23, "Chicago"),
]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()

# Store the DataFrame into a Hive table
table_name = "person_info"
df.write.mode("overwrite").saveAsTable(table_name)

# Verify the table
spark.sql(f"SELECT * FROM {table_name}").show()

# Stop the Spark session



# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from person_info
# MAGIC

# COMMAND ----------

df_filtered = df.filter(df.age > 25)
print("Filtered DataFrame (age > 25):")
df_filtered.show()
