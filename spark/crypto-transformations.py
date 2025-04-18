#!/usr/bin/env python3
"""
Crypto Price Transformations with PySpark

This script implements various price transformations on cryptocurrency data:
1. Price indices (normalized to 100 at start)
2. Volatility metrics (rolling standard deviation)
3. Beta calculation relative to Bitcoin
"""

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, first, last, avg, stddev, lag, datediff, to_date
from pyspark.sql.functions import coalesce, when, count, min, max, expr, sum, corr
from pyspark.conf import SparkConf

# Configuration
PROJECT_ID = "data-pipelines-450717"
TEMP_GCS_BUCKET = f"{PROJECT_ID}-spark-temp"
CREDENTIALS_PATH = "/etc/gcp-credentials/service-account.json"
HISTORICAL_DATA_TABLE = f"{PROJECT_ID}.coinbase_data.historical_data"
BITCOIN_ID = "BTC-USD"  # The ID for Bitcoin in your data

def create_spark_session():
    """Create a Spark session configured for BigQuery access"""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
    
    conf = SparkConf()
    conf.set("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
    conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)
    
    spark = SparkSession.builder \
        .appName("Crypto Analysis") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark

def read_historical_data(spark):
    """Read historical price data from BigQuery"""
    return spark.read.format("bigquery") \
        .option("table", HISTORICAL_DATA_TABLE) \
        .load()

def calculate_price_indices(df):
    """
    Calculate price indices normalized to 100 at the earliest date for each currency
    """
    # Define window for each cryptocurrency, ordered by timestamp
    window_spec = Window.partitionBy("id").orderBy("timestamp")
    
    # Get first price for each cryptocurrency
    df_with_first = df.withColumn("first_price", first("close").over(window_spec))
    
    # Calculate price index (current price / first price * 100)
    price_indices = df_with_first.withColumn(
        "price_index", 
        (col("close") / col("first_price") * 100)
    )
    
    # Select relevant columns
    return price_indices.select(
        "id", 
        "timestamp", 
        "close", 
        "price_index",
        "partition_date"
    )

def calculate_volatility_metrics(df, window_days=7):
    """
    Calculate rolling volatility metrics:
    - Standard deviation over n-day window
    - Coefficient of variation (std/mean)
    """
    # Convert timestamp to date if needed
    df = df.withColumn("date", to_date(col("timestamp")))
    
    # Define window spec for rows between current and n days prior
    # Using row-based window rather than range window to avoid data type issues
    rows_window = Window.partitionBy("id").orderBy("date")
    
    # First, number the rows within each partition to use row numbers instead of date arithmetic
    df_with_rows = df.withColumn("row_num", count("*").over(Window.partitionBy("id").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)))
    
    # Now create a window based on row numbers
    row_based_window = Window.partitionBy("id").orderBy("row_num").rowsBetween(-window_days, 0)
    
    # Calculate rolling metrics
    volatility_df = df_with_rows.withColumn(
        f"volatility_{window_days}d", 
        stddev("close").over(row_based_window)
    ).withColumn(
        f"avg_price_{window_days}d",
        avg("close").over(row_based_window)
    ).withColumn(
        f"coefficient_variation_{window_days}d",
        col(f"volatility_{window_days}d") / col(f"avg_price_{window_days}d")
    ).withColumn(
        f"price_range_{window_days}d",
        (max("close").over(row_based_window) - min("close").over(row_based_window)) / 
        coalesce(min("close").over(row_based_window), lit(1)) * 100
    )
    
    # Select relevant columns
    return volatility_df.select(
        "id", 
        "timestamp", 
        "close",
        f"volatility_{window_days}d",
        f"coefficient_variation_{window_days}d",
        f"price_range_{window_days}d",
        "partition_date"
    )

def calculate_beta(df, window_days=30):
    """
    Calculate beta coefficient for each cryptocurrency relative to Bitcoin
    
    Beta = Covariance(Crypto, BTC) / Variance(BTC)
    """
    # Get daily returns
    window_prev_day = Window.partitionBy("id").orderBy("timestamp")
    
    df_with_returns = df.withColumn(
        "prev_close", 
        lag("close", 1).over(window_prev_day)
    ).filter(col("prev_close").isNotNull()).withColumn(
        "daily_return", 
        (col("close") - col("prev_close")) / col("prev_close")
    )
    
    # Extract Bitcoin returns
    btc_returns = df_with_returns.filter(col("id") == BITCOIN_ID).select(
        "timestamp", 
        col("daily_return").alias("btc_return")
    )
    
    # Join bitcoin returns with other crypto returns
    all_returns = df_with_returns.join(
        btc_returns, 
        on="timestamp",
        how="inner"
    )
    
    # Add row numbering for window calculation
    all_returns_with_rows = all_returns.withColumn(
        "date", 
        to_date(col("timestamp"))
    ).withColumn(
        "row_num", 
        count("*").over(Window.partitionBy("id").orderBy("date").rowsBetween(Window.unboundedPreceding, 0))
    )
    
    # Define window based on row numbers instead of dates
    beta_window = Window.partitionBy("id").orderBy("row_num").rowsBetween(-window_days, 0)
    
    # Calculate beta using correlation and standard deviations
    beta_df = all_returns_with_rows.withColumn(
        "correlation", 
        corr("daily_return", "btc_return").over(beta_window)
    ).withColumn(
        "crypto_stddev", 
        stddev("daily_return").over(beta_window)
    ).withColumn(
        "btc_stddev", 
        stddev("btc_return").over(beta_window)
    ).withColumn(
        f"beta_{window_days}d", 
        col("correlation") * (col("crypto_stddev") / col("btc_stddev"))
    )
    
    # Select relevant columns
    return beta_df.select(
        "id", 
        "timestamp", 
        "close",
        "daily_return",
        "btc_return",
        f"beta_{window_days}d",
        "partition_date"
    )

def run_all_transformations(write_to_bq=True):
    """Run all transformations and optionally write results to BigQuery"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read historical data
        print("Reading historical data...")
        historical_data = read_historical_data(spark)
        historical_data.cache()  # Cache for multiple uses
        
        # 1. Price Indices
        print("Calculating price indices...")
        price_indices_df = calculate_price_indices(historical_data)
        price_indices_df.show(5)
        
        if write_to_bq:
            print("Writing price indices to BigQuery...")
            price_indices_df.write.format("bigquery") \
                .option("table", f"{PROJECT_ID}.coinbase_data.price_indices") \
                .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
                .mode("overwrite") \
                .save()
        
        # 2. Volatility Metrics
        print("Calculating volatility metrics (7-day window)...")
        volatility_df = calculate_volatility_metrics(historical_data, window_days=7)
        volatility_df.show(5)
        
        if write_to_bq:
            print("Writing volatility metrics to BigQuery...")
            volatility_df.write.format("bigquery") \
                .option("table", f"{PROJECT_ID}.coinbase_data.volatility_metrics") \
                .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
                .mode("overwrite") \
                .save()
        
        # 3. Beta Calculation
        print("Calculating beta relative to Bitcoin (30-day window)...")
        beta_df = calculate_beta(historical_data, window_days=30)
        beta_df.show(5)
        
        if write_to_bq:
            print("Writing beta metrics to BigQuery...")
            beta_df.write.format("bigquery") \
                .option("table", f"{PROJECT_ID}.coinbase_data.beta_metrics") \
                .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
                .mode("overwrite") \
                .save()
            
        # Uncache the data
        historical_data.unpersist()
        
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    run_all_transformations(write_to_bq=True)
