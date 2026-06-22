# ================================================================
# 03_gold_aggregate.py - Gold Layer Notebook
# Interactive notebook for exploring and validating gold layer aggregations
# This notebook mirrors the gold layer logic in the DLT pipeline but allows
# for manual exploration and debugging inside the Databricks workspace
# ================================================================

# Databricks notebook source
# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import col, avg, sum, count, round

# COMMAND ----------

# Read from the silver Delta table written in notebook 02
silver_df = spark.read.format("delta").table("nyc_taxi_dev.silver_taxi_trips")

# Preview the silver data
silver_df.display()

# COMMAND ----------

# Build monthly summary aggregations
# This is the business level reporting layer consumed by analysts and dashboards
gold_df = (
    silver_df
    .groupBy("pickup_year", "pickup_month")
    .agg(
        # Total number of trips per month
        count("*").alias("total_trips"),
        # Average trip distance per month
        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        # Total revenue per month
        round(sum("fare_amount"), 2).alias("total_fare_revenue"),
        # Total passengers per month
        sum("passenger_count").alias("total_passengers")
    )
    # Sort by year and month for readability
    .orderBy("pickup_year", "pickup_month")
)

# COMMAND ----------

# Preview the gold aggregations
gold_df.display()

# COMMAND ----------

# Validate row counts - should have one row per year/month combination
print(f"Total year/month combinations: {gold_df.count()}")

# COMMAND ----------

# Spot check - verify total trips aligns with silver row count
silver_count = silver_df.count()
gold_total_trips = gold_df.agg(sum("total_trips")).collect()[0][0]

print(f"Silver row count:        {silver_count:,}")
print(f"Gold total trips summed: {gold_total_trips:,}")
print(f"Counts match:            {silver_count == gold_total_trips}")

# COMMAND ----------

# Write aggregations to Delta Lake gold table
gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("nyc_taxi_dev.gold_monthly_summary")

print("Gold table written successfully")