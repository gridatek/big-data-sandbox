#!/usr/bin/env python3
"""
ETL Pipeline Example - Complete Extract, Transform, Load workflow
Demonstrates enterprise-grade batch processing patterns with Spark and MinIO
"""

import sys
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, app_name="ETL_Pipeline"):
        """Initialize Spark session with MinIO configuration"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("spark://spark-master:7077") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session initialized: {self.spark.version}")

    def extract_data(self, source_path):
        """Extract data from various sources"""
        logger.info(f"Extracting data from: {source_path}")

        try:
            if source_path.endswith('.csv'):
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            elif source_path.endswith('.json'):
                df = self.spark.read.json(source_path)
            elif source_path.endswith('.parquet'):
                df = self.spark.read.parquet(source_path)
            else:
                # Try S3 path
                if source_path.startswith('s3a://'):
                    df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
                else:
                    raise ValueError(f"Unsupported file format: {source_path}")

            logger.info(f"Successfully extracted {df.count()} records")
            return df

        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            raise

    def validate_data(self, df, required_columns=None):
        """Perform data quality checks"""
        logger.info("Performing data quality validation...")

        validation_results = {
            'total_records': df.count(),
            'columns': df.columns,
            'null_counts': {},
            'duplicate_count': 0,
            'validation_passed': True,
            'issues': []
        }

        # Check for required columns
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                validation_results['validation_passed'] = False
                validation_results['issues'].append(f"Missing required columns: {missing_cols}")

        # Check for null values
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            validation_results['null_counts'][col_name] = null_count

            if null_count > 0:
                validation_results['issues'].append(f"Column '{col_name}' has {null_count} null values")

        # Check for duplicates (if primary key columns exist)
        if 'transaction_id' in df.columns:
            total_records = df.count()
            unique_records = df.select('transaction_id').distinct().count()
            validation_results['duplicate_count'] = total_records - unique_records

            if validation_results['duplicate_count'] > 0:
                validation_results['validation_passed'] = False
                validation_results['issues'].append(f"Found {validation_results['duplicate_count']} duplicate records")

        # Log validation results
        if validation_results['validation_passed']:
            logger.info("✅ Data validation passed")
        else:
            logger.warning(f"⚠️ Data validation issues found: {validation_results['issues']}")

        return validation_results

    def transform_sales_data(self, df):
        """Transform sales data with business logic"""
        logger.info("Applying business transformations...")

        # Add calculated columns
        transformed_df = df.withColumn("total_amount", col("quantity") * col("price")) \
                          .withColumn("transaction_date", to_date(col("date"), "yyyy-MM-dd")) \
                          .withColumn("year", year("transaction_date")) \
                          .withColumn("month", month("transaction_date")) \
                          .withColumn("quarter", quarter("transaction_date"))

        # Add business categories
        transformed_df = transformed_df.withColumn(
            "price_category",
            when(col("price") < 50, "Budget")
            .when(col("price") < 200, "Mid-range")
            .when(col("price") < 500, "Premium")
            .otherwise("Luxury")
        )

        # Add customer value segments (requires aggregation)
        customer_totals = transformed_df.groupBy("customer_id") \
            .agg(sum("total_amount").alias("customer_lifetime_value"))

        customer_segments = customer_totals.withColumn(
            "customer_segment",
            when(col("customer_lifetime_value") > 1000, "VIP")
            .when(col("customer_lifetime_value") > 500, "Gold")
            .when(col("customer_lifetime_value") > 100, "Silver")
            .otherwise("Bronze")
        )

        # Join back to main dataset
        final_df = transformed_df.join(customer_segments, "customer_id", "left")

        logger.info("✅ Business transformations completed")
        return final_df

    def aggregate_metrics(self, df):
        """Create business metrics and KPIs"""
        logger.info("Calculating business metrics...")

        metrics = {}

        # Overall metrics
        metrics['total_revenue'] = df.agg(sum("total_amount")).collect()[0][0]
        metrics['total_transactions'] = df.count()
        metrics['unique_customers'] = df.select("customer_id").distinct().count()
        metrics['avg_transaction_value'] = metrics['total_revenue'] / metrics['total_transactions']

        # Category performance
        category_metrics = df.groupBy("category") \
            .agg(
                sum("total_amount").alias("category_revenue"),
                count("transaction_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_value")
            ) \
            .orderBy(col("category_revenue").desc())

        # Regional performance
        regional_metrics = df.groupBy("region") \
            .agg(
                sum("total_amount").alias("region_revenue"),
                countDistinct("customer_id").alias("unique_customers")
            ) \
            .orderBy(col("region_revenue").desc())

        # Monthly trends
        monthly_trends = df.groupBy("year", "month") \
            .agg(
                sum("total_amount").alias("monthly_revenue"),
                count("transaction_id").alias("monthly_transactions")
            ) \
            .orderBy("year", "month")

        logger.info("✅ Business metrics calculated")
        return {
            'overall_metrics': metrics,
            'category_metrics': category_metrics,
            'regional_metrics': regional_metrics,
            'monthly_trends': monthly_trends
        }

    def load_data(self, df, output_path, format="parquet", mode="overwrite"):
        """Load transformed data to destination"""
        logger.info(f"Loading data to: {output_path}")

        try:
            if format.lower() == "parquet":
                df.write.mode(mode).parquet(output_path)
            elif format.lower() == "csv":
                df.write.mode(mode).option("header", "true").csv(output_path)
            elif format.lower() == "json":
                df.write.mode(mode).json(output_path)
            else:
                raise ValueError(f"Unsupported format: {format}")

            logger.info("✅ Data loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise

    def run_pipeline(self, source_path, output_base_path, run_validation=True):
        """Execute the complete ETL pipeline"""
        logger.info("🚀 Starting ETL Pipeline")
        start_time = datetime.now()

        try:
            # Extract
            raw_df = self.extract_data(source_path)

            # Validate (optional)
            if run_validation:
                validation_results = self.validate_data(raw_df,
                    required_columns=['transaction_id', 'customer_id', 'product_name', 'quantity', 'price'])

                if not validation_results['validation_passed']:
                    logger.warning("Data quality issues detected, but continuing...")

            # Transform
            transformed_df = self.transform_sales_data(raw_df)

            # Generate metrics
            metrics = self.aggregate_metrics(transformed_df)

            # Load transformed data
            self.load_data(transformed_df, f"{output_base_path}/processed_sales", "parquet")

            # Load metrics
            self.load_data(metrics['category_metrics'], f"{output_base_path}/category_metrics", "parquet")
            self.load_data(metrics['regional_metrics'], f"{output_base_path}/regional_metrics", "parquet")
            self.load_data(metrics['monthly_trends'], f"{output_base_path}/monthly_trends", "parquet")

            # Calculate runtime
            end_time = datetime.now()
            runtime = (end_time - start_time).total_seconds()

            logger.info(f"🎉 ETL Pipeline completed successfully in {runtime:.2f} seconds")

            # Print summary
            print("\n" + "="*60)
            print("📊 ETL PIPELINE SUMMARY")
            print("="*60)
            print(f"📁 Source: {source_path}")
            print(f"📁 Output: {output_base_path}")
            print(f"⏱️  Runtime: {runtime:.2f} seconds")
            print(f"📊 Records processed: {metrics['overall_metrics']['total_transactions']:,}")
            print(f"💰 Total revenue: ${metrics['overall_metrics']['total_revenue']:,.2f}")
            print(f"👥 Unique customers: {metrics['overall_metrics']['unique_customers']:,}")
            print(f"🛒 Avg transaction: ${metrics['overall_metrics']['avg_transaction_value']:.2f}")
            print("="*60)

            return {
                'status': 'success',
                'runtime_seconds': runtime,
                'metrics': metrics['overall_metrics'],
                'output_path': output_base_path
            }

        except Exception as e:
            logger.error(f"❌ ETL Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description='ETL Pipeline for Big Data Sandbox')
    parser.add_argument('--source', required=True, help='Source data path')
    parser.add_argument('--output', required=True, help='Output base path')
    parser.add_argument('--skip-validation', action='store_true', help='Skip data validation')

    args = parser.parse_args()

    pipeline = ETLPipeline()

    try:
        result = pipeline.run_pipeline(
            source_path=args.source,
            output_base_path=args.output,
            run_validation=not args.skip_validation
        )

        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)

    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    # Example usage when run directly
    pipeline = ETLPipeline()

    try:
        # Run with sample data
        result = pipeline.run_pipeline(
            source_path="/data/sales_data.csv",
            output_base_path="s3a://processed",
            run_validation=True
        )

        print(f"\n🎯 Pipeline result: {result['status']}")

    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
    finally:
        pipeline.cleanup()