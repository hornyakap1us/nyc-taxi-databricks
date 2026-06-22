# ==============================================================================
# test_data_quality.py - Data Quality Tests
# Pytest tests that validate the Delta tables produced by the pipeline
# These tests run automatically in GitHub Actions before every deployment
# If any test fails the deployment is blocked until the issue is resolved
# ==============================================================================

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# ==============================================================================
# SETUP - Create a local Spark session for testing
# This runs before any tests execute and is shared across all tests
# ==============================================================================

@pytest.fixture(scope="session")
def spark():
    # Create a local Spark session for running tests
    # This is separate from the Databricks cluster Spark session
    return SparkSession.builder \
        .appName("nyc_taxi_data_quality_tests") \
        .master("local") \
        .getOrCreate()
    
# ==============================================================================
# BRONZE LAYER TESTS
# Validate the raw ingestion layer
# ==============================================================================

def test_bronze_table_exists(spark):
    """Bronze table should exist and be readable"""
    df = spark.read.format("delta").load("/tmp/bronze_taxi_trips")
    assert df is not None

def test_bronze_row_count(spark):
    """Bronze table should have at least 1 million rows"""
    df = spark.read.format("delta").load("/tmp/bronze_taxi_trips")
    assert df.count() > 1_000_000

def test_bronze_expected_columns(spark):
    """Bronze table should contain all expected raw columns"""
    df = spark.read.format("delta").load("/tmp/bronze_taxi_trips")
    expected_columns = [
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]
    for column in expected_columns:
        assert column in df.columns, f"Missing expected column: {column}"

# ==============================================================================
# SILVER LAYER TESTS
# Validate the cleaned and transformed layer
# ==============================================================================

def test_silver_no_negative_fares(spark):
    """Silver table should have no negative fare amounts"""
    df = spark.read.format("delta").load("/tmp/silver_taxi_trips")
    negative_fares = df.filter(col("fare_amount") < 0).count()
    assert negative_fares == 0, f"Found {negative_fares} negative fare amounts"

def test_silver_no_negative_passenger_count(spark):
    """Silver table should have no zero or negative passenger counts"""
    df = spark.read.format("delta").load("/tmp/silver_taxi_trips")
    invalid = df.filter(col("passenger_count") <= 0).count()
    assert invalid == 0, f"Found {invalid} invalid passenger counts"

def test_silver_no_null_pickup_datetime(spark):
    """Silver table should have no null pickup timestamps"""
    df = spark.read.format("delta").load("/tmp/silver_taxi_trips")
    nulls = df.filter(col("pickup_datetime").isNull()).count()
    assert nulls == 0, f"Found {nulls} null pickup timestamps"

def test_silver_has_year_and_month_columns(spark):
    """Silver table should have pickup_year and pickup_month columns"""
    df = spark.read.format("delta").load("/tmp/silver_taxi_trips")
    assert "pickup_year" in df.columns
    assert "pickup_month" in df.columns

# ==============================================================================
# GOLD LAYER TESTS
# Validate the business level aggregation layer
# ==============================================================================

def test_gold_row_count(spark):
    """Gold table should have at least 12 rows - one per month minimum"""
    df = spark.read.format("delta").load("/tmp/gold_monthly_summary")
    assert df.count() >= 12

def test_gold_no_null_trip_counts(spark):
    """Gold table should have no null total_trips values"""
    df = spark.read.format("delta").load("/tmp/gold_monthly_summary")
    nulls = df.filter(col("total_trips").isNull()).count()
    assert nulls == 0, f"Found {nulls} null trip counts in gold table"

def test_gold_no_negative_revenue(spark):
    """Gold table should have no negative total fare revenue"""
    df = spark.read.format("delta").load("/tmp/gold_monthly_summary")
    negative = df.filter(col("total_fare_revenue") < 0).count()
    assert negative == 0, f"Found {negative} negative revenue rows"

def test_gold_trip_counts_match_silver(spark):
    """Gold total trips summed should match silver row count exactly"""
    silver_df = spark.read.format("delta").load("/tmp/silver_taxi_trips")
    gold_df = spark.read.format("delta").load("/tmp/gold_monthly_summary")
    silver_count = silver_df.count()
    gold_total = gold_df.agg(sum("total_trips")).collect()[0][0]
    assert silver_count == gold_total, \
        f"Row count mismatch: silver={silver_count:,} gold={gold_total:,}"