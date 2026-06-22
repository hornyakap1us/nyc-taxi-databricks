# ======================================================
# 02_silver_transform.py - Silver Layer Notebook
# Interactive notebook for exploring and validating silver layer transformations
# This notebook mirrors the silver layer logic in the DLT pipeline but allows
# for manual exploration and debugging inside the Databricks workspace
# ======================================================

# Databricks notebook source
# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import col, to_timestamp, year, month, count, when

# COMMAND ----------

# Read from the bronze Delta table written in notebook 01
bronze_df = spark.read.format("delta").table("nyc_taxi_dev.bronze_taxi_trips")

# Preview the bronze data
bronze_df.display()

# COMMAND ----------

# Convert raw string timestamps to proper timestamp types
# and extract year and month for partitioning and aggregation
silver_df = (
    bronze_df
    # Convert raw string timestamps to proper timestamp types
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    # Extract year and month for partitioning and aggregation
    .withColumn("pickup_year", year(col("pickup_datetime")))
    .withColumn("pickup_month", month(col("pickup_datetime")))
    # Remove duplicate records
    .dropDuplicates()
    # Drop the original raw timestamp columns now that we have clean ones
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
)

# COMMAND ----------

# Apply data quality filters matching the DLT pipeline expectations
# Records failing these checks are dropped from the silver layer
silver_df = (
    silver_df
    .filter(col("passenger_count") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") > 0)
)

# COMMAND ----------

# Validate the transformations look correct
silver_df.display()

# COMMAND ----------

# Check the schema to confirm timestamp conversions worked correctly
silver_df.printSchema()

# COMMAND ----------

# Compare row counts between bronze and silver
# The difference represents records dropped by data quality filters
bronze_count = bronze_df.count()
silver_count = silver_df.count()
dropped = bronze_count - silver_count

print(f"Bronze row count: {bronze_count:,}")
print(f"Silver row count: {silver_count:,}")
print(f"Records dropped: {dropped:,}")
print(f"Drop rate: {dropped / bronze_count * 100:.2f}%")

# COMMAND ----------

# Write cleaned data to Delta Lake silver table
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_dev.silver_taxi_trips")

print("Silver table written successfully")