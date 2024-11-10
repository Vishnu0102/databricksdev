# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pow

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SquareCalculation") \
    .getOrCreate()

# Sample data with integer values
data = [
    (1,),
    (2,),
    (3,),
    (4,),
    (5,)
]

# Define the schema
schema = ["number"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show original DataFrame
print("Original DataFrame:")
df.show()

# Add a new column with the square of the numbers
df_with_squares = df.withColumn("square", pow(col("number"), 2))

# Show DataFrame with the square column
print("DataFrame with Square of Numbers:")
df_with_squares.show()

# Stop the Spark session


