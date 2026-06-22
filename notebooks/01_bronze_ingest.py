# =================================================================================
# 01_bronze_ingest.py - Bronze Layer Notebook
# Interactive notebook for exploring and validating raw NYC taxi data ingestion
# This notebook mirrors the bronze layer logic in the DLT pipeline but allows
# for manual exploration and debugging inside the Databricks workspace
# ==================================================================================

# Databricks notebook source
# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import col, count, when

# COMMAND ----------

# Load raw NYC taxi data from Databricks sample datasets
# This is the same path used in the DLT pipeline
raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/databricks-datasets/nyctaxi/tripdata/yellow/")
)

# COMMAND ----------

# Preview the raw data
# Always a good first step to understand what you are working with

raw_df.display()

# COMMAND ----------

# Check the schema to confirm data types were inferred correctly
# Key columns to verify: fare_amount (double), passenger_count(int),
# tpep_pickup_datetime and tpep_dropoff_datetime (timestamp or string)
raw_df.printSchema()

# COMMAND ----------

# Check row count to understand the size of the dataset
print(f"Total raw records: {raw_df.count():,}")

# COMMAND ----------

# Check for nulls across all columns
# This helps identify data quality issues before transformation
null_counts = raw_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in raw_df.columns]
)
null_counts.display()

# Purpose of above code
# raw_df.columns - This is simply a list of all column names in your dataframe. 
# for c in raw_df.columns - This is the list comprehension loop — it iterates over every column name in the dataframe. So c takes the value of each column name one at a time, like "passenger_count", then "trip_distance", and so on.
# col(c) - This converts the column name string c into an actual Spark column object that Spark can work with. So col("fare_amount") means "give me the fare_amount column".
# .isNull() - This checks whether each value in the column is null. Returns True if the value is null, False if it has a value.
# when(col(c).isNull(), c) - This is a conditional statement — think of it as an IF statement. It's saying:
#      - IF the value in column c is null
#      - THEN return the column name c
#      - OTHERWISE return null implicitly
# So it only returns a value when the cell is null, and nothing when it isn't.
# count(...) - This counts how many times when returned a value — in other words, how many nulls exist in that column. Non-null values were returned as null by when so they don't get counted.
# .alias(c) - This renames the resulting count column to the original column name so the output is readable. Without this every column would just be named count(CASE WHEN...) which is meaningless.
# The list comprehension [... for c in raw_df.columns] - This runs the entire count(when(col(c).isNull(), c)).alias(c) expression for every column in the dataframe and collects the results into a list. That list gets passed to .select() which builds a new dataframe with one column per original column, each showing its null count.
# Putting it all together in plain English: - "For every column in the dataframe, count how many rows have a null value in that column, and return the results as a single row showing the null count for each column."



# COMMAND ----------

# Write raw data to Delta Lake bronze table
# mode overwrite replaces the table each time this notebook runs
raw_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_dev.bronze_taxi_trips")

print("Bronze table written successfully")
