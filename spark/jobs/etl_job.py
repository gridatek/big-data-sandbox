"""
Sample Spark ETL Job for Big Data Sandbox
Processes sales data from MinIO and writes aggregated results back
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from datetime import datetime

def create_spark_session():
    """
    Create and configure Spark session with MinIO support
    """
    return SparkSession.builder \
        .appName("SampleETLJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_sales_data(spark, input_path):
    """
    Read sales data from MinIO
    """
    print(f"Reading data from: {input_path}")

    # Define schema for better performance
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("region", StringType(), True)
    ])

    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)

    print(f"Loaded {df.count()} records")
    df.printSchema()

    return df

def transform_sales_data(df):
    """
    Apply transformations to sales data
    """
    print("Applying transformations...")

    # Convert date string to proper date type
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # Calculate total sale amount
    df = df.withColumn("total_amount", col("quantity") * col("price"))

    # Add year and month for partitioning
    df = df.withColumn("year", year("date"))
    df = df.withColumn("month", month("date"))

    # Filter out invalid records
    df = df.filter(
        (col("quantity") > 0) &
        (col("price") > 0) &
        (col("customer_id").isNotNull())
    )

    # Add processing timestamp
    df = df.withColumn("processed_at", current_timestamp())

    return df

def generate_aggregations(df):
    """
    Generate various aggregations for business insights
    """
    print("Generating aggregations...")

    # Daily sales by region
    daily_regional_sales = df.groupBy("date", "region") \
        .agg(
            sum("total_amount").alias("total_sales"),
            count("transaction_id").alias("transaction_count"),
            avg("total_amount").alias("avg_transaction_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("date", "region")

    # Monthly product performance
    monthly_product_sales = df.groupBy("year", "month", "product_id", "category") \
        .agg(
            sum("quantity").alias("total_quantity"),
            sum("total_amount").alias("total_revenue"),
            count("transaction_id").alias("transaction_count")
        ) \
        .orderBy("year", "month", "total_revenue", ascending=[True, True, False])

    # Customer segmentation
    customer_segments = df.groupBy("customer_id") \
        .agg(
            sum("total_amount").alias("lifetime_value"),
            count("transaction_id").alias("purchase_count"),
            avg("total_amount").alias("avg_purchase_value"),
            max("date").alias("last_purchase_date")
        ) \
        .withColumn("customer_segment",
            when(col("lifetime_value") > 10000, "VIP")
            .when(col("lifetime_value") > 5000, "Gold")
            .when(col("lifetime_value") > 1000, "Silver")
            .otherwise("Bronze")
        )

    return {
        "daily_regional_sales": daily_regional_sales,
        "monthly_product_sales": monthly_product_sales,
        "customer_segments": customer_segments
    }

def write_to_minio(dataframes, output_base_path):
    """
    Write processed data back to MinIO
    """
    print(f"Writing results to: {output_base_path}")

    for name, df in dataframes.items():
        output_path = f"{output_base_path}/{name}"
        print(f"Writing {name} to {output_path}")

        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .partitionBy("year", "month") if "year" in df.columns else df.write.mode("overwrite") \
            .parquet(output_path)

        print(f"Successfully wrote {name}")

def generate_data_quality_metrics(df):
    """
    Generate data quality metrics
    """
    total_records = df.count()
    null_customer_ids = df.filter(col("customer_id").isNull()).count()
    negative_quantities = df.filter(col("quantity") <= 0).count()
    negative_prices = df.filter(col("price") <= 0).count()

    quality_metrics = {
        "total_records": total_records,
        "null_customer_ids": null_customer_ids,
        "negative_quantities": negative_quantities,
        "negative_prices": negative_prices,
        "data_quality_score": 1 - ((null_customer_ids + negative_quantities + negative_prices) / (total_records * 3))
    }

    print("\n=== Data Quality Metrics ===")
    for metric, value in quality_metrics.items():
        print(f"{metric}: {value}")

    return quality_metrics

def main():
    """
    Main ETL pipeline execution
    """
    # Initialize Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Configuration
        input_path = "s3a://raw-data/sales_data.csv"
        output_base_path = "s3a://processed/sales_analysis"

        # Read data
        raw_df = read_sales_data(spark, input_path)

        # Generate quality metrics
        quality_metrics = generate_data_quality_metrics(raw_df)

        # Transform data
        transformed_df = transform_sales_data(raw_df)

        # Show sample of transformed data
        print("\n=== Sample Transformed Data ===")
        transformed_df.show(10, truncate=False)

        # Generate aggregations
        aggregations = generate_aggregations(transformed_df)

        # Show sample aggregations
        print("\n=== Daily Regional Sales Sample ===")
        aggregations["daily_regional_sales"].show(10)

        print("\n=== Top Products by Revenue ===")
        aggregations["monthly_product_sales"].show(10)

        print("\n=== Customer Segmentation Sample ===")
        aggregations["customer_segments"].show(10)

        # Write results to MinIO
        write_to_minio(aggregations, output_base_path)

        print("\n=== ETL Pipeline Completed Successfully ===")
        print(f"Processing timestamp: {datetime.now()}")
        print(f"Quality score: {quality_metrics['data_quality_score']:.2%}")

    except Exception as e:
        print(f"Error in ETL pipeline: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()