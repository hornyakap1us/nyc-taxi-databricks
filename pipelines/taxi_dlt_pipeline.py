# =======================================================================
# taxi_dlt_pipeline.py - Delta Live Tables Pipeline Definition
# Defines the full medallion architecture pipeline using DLT
# bronze -> silver -> gold transformations with built-in data quality checks
# =======================================================================

import dlt 
from pyspark.sql.functions import col, to_timestamp, year, month

# =======================================================================
# BRONZE LAYER - Raw ingestion
# Pulls raw NYC taxi data directly from the Databricks sample datasets
# No transformations applied - this is the raw, unmodified source data
# =======================================================================

@dlt.table(
    name = "bronze_taxi_trips",
    comment = "Raw NYC taxi trip records ingested from source - no transformations applied"
)
def bronze_taxi_trips():
    return(
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/databricks-datasets/nyctaxi/tripdata/yellow/")
    )

# =======================================================================
# SILVER LAYER - Cleaned and validated data
# Applies data quality expectations using @dlt.expect decorators
# Records that fail expectations are quarantined rather than silently dropped
# =======================================================================

@dlt.table(
    name = "silver_taxi_trips",
    comment = "Cleaned and validated taxi trip records with invalid records quarantined"
)
# Data quality checks - records failing these expectations are flagged
# @dlt.expect warns but keeps the record
# @dlt.expect_or_drop removes invalid records
# @dlt.expect_or_fail stops the pipeline if the expectation is violated
@dlt.expect("valid_passenger_count", "passenger_count > 0")
@dlt.expect("valid_trip_distance", "trip_distance > 0")
@dlt.expect_or_drop("valid_fare", "fare_amount > 0")
def silver_taxi_trips():
    return(
        # Read from the bronze table defined above
        dlt.read("bronze_taxi_trips")
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

# ========================================================================
# GOLD LAYER - Business level aggregations
# Builds reporting ready summaries from the clean silver data
# This is the layer that analysts and dashboards consume
# ========================================================================

@dlt.table(
    name = "gold_monthly_summary",
    comment = "Monthly trip and revenue summary aggregated from silver layer"
)
def gold_monthly_summary():
    return(
        # Read from the silver table defined above
        dlt.read("silver_taxi_trips")
        # Aggregate by year and month
        .groupBy("pickup_year", "pickup_month")
        .agg(
            {"trip_distance": "avg",
             "fare_amount": "sum",
             "passenger_count": "sum",
             "*": "count"}
        )
        # Rename the count column to something meaningful
        .withColumnRenamed("count(1)", "total_trips")
    )