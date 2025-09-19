#!/usr/bin/env python3
"""
Advanced Analytics Job - Statistical Analysis and Business Intelligence
Demonstrates sophisticated analytics patterns with PySpark
"""

import sys
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedAnalytics:
    def __init__(self, app_name="Advanced_Analytics"):
        """Initialize Spark session with ML libraries"""
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
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Advanced Analytics session initialized: {self.spark.version}")

    def load_data(self, data_path):
        """Load and prepare data for analysis"""
        logger.info(f"Loading data from: {data_path}")

        try:
            if data_path.endswith('.csv'):
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
            else:
                df = self.spark.read.parquet(data_path)

            # Basic data preparation
            df = df.withColumn("total_amount", col("quantity") * col("price")) \
                   .withColumn("transaction_date", to_date(col("date"), "yyyy-MM-dd")) \
                   .withColumn("day_of_week", dayofweek("transaction_date")) \
                   .withColumn("is_weekend", col("day_of_week").isin([1, 7]))

            logger.info(f"Data loaded successfully: {df.count()} records")
            return df

        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise

    def cohort_analysis(self, df):
        """Perform customer cohort analysis"""
        logger.info("Performing cohort analysis...")

        # Define customer first purchase date
        customer_first_purchase = df.groupBy("customer_id") \
            .agg(min("transaction_date").alias("first_purchase_date"))

        # Join back to transactions
        cohort_df = df.join(customer_first_purchase, "customer_id")

        # Calculate months since first purchase
        cohort_df = cohort_df.withColumn(
            "months_since_first_purchase",
            months_between("transaction_date", "first_purchase_date")
        )

        # Create cohort table
        cohort_table = cohort_df.groupBy("first_purchase_date", "months_since_first_purchase") \
            .agg(countDistinct("customer_id").alias("customers")) \
            .orderBy("first_purchase_date", "months_since_first_purchase")

        logger.info("✅ Cohort analysis completed")
        return cohort_table

    def rfm_analysis(self, df):
        """Perform RFM (Recency, Frequency, Monetary) analysis"""
        logger.info("Performing RFM analysis...")

        analysis_date = df.agg(max("transaction_date")).collect()[0][0]

        # Calculate RFM metrics
        rfm_df = df.groupBy("customer_id") \
            .agg(
                max("transaction_date").alias("last_purchase_date"),
                count("transaction_id").alias("frequency"),
                sum("total_amount").alias("monetary_value")
            ) \
            .withColumn(
                "recency",
                datediff(lit(analysis_date), col("last_purchase_date"))
            )

        # Create RFM scores (quintiles)
        window_spec = Window.orderBy("recency")
        rfm_scored = rfm_df.withColumn("recency_rank", ntile(5).over(window_spec))

        window_spec = Window.orderBy(col("frequency").desc())
        rfm_scored = rfm_scored.withColumn("frequency_rank", ntile(5).over(window_spec))

        window_spec = Window.orderBy(col("monetary_value").desc())
        rfm_scored = rfm_scored.withColumn("monetary_rank", ntile(5).over(window_spec))

        # Create RFM segments
        rfm_scored = rfm_scored.withColumn(
            "rfm_segment",
            when((col("recency_rank") >= 4) & (col("frequency_rank") >= 4) & (col("monetary_rank") >= 4), "Champions")
            .when((col("recency_rank") >= 3) & (col("frequency_rank") >= 3) & (col("monetary_rank") >= 4), "Loyal Customers")
            .when((col("recency_rank") >= 4) & (col("frequency_rank") <= 2), "New Customers")
            .when((col("recency_rank") >= 3) & (col("frequency_rank") <= 2) & (col("monetary_rank") >= 3), "Potential Loyalists")
            .when((col("recency_rank") >= 4) & (col("frequency_rank") >= 3) & (col("monetary_rank") <= 2), "At Risk")
            .when((col("recency_rank") <= 2) & (col("frequency_rank") >= 2) & (col("monetary_rank") >= 2), "Cannot Lose Them")
            .when((col("recency_rank") <= 2) & (col("frequency_rank") <= 2), "Hibernating")
            .otherwise("Others")
        )

        logger.info("✅ RFM analysis completed")
        return rfm_scored

    def customer_clustering(self, df):
        """Perform customer segmentation using K-means clustering"""
        logger.info("Performing customer clustering...")

        # Prepare features for clustering
        customer_features = df.groupBy("customer_id") \
            .agg(
                sum("total_amount").alias("total_spent"),
                count("transaction_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_value"),
                countDistinct("product_name").alias("product_diversity"),
                max("transaction_date").alias("last_purchase"),
                min("transaction_date").alias("first_purchase")
            ) \
            .withColumn(
                "customer_lifetime_days",
                datediff("last_purchase", "first_purchase") + 1
            ) \
            .withColumn(
                "purchase_frequency",
                col("transaction_count") / col("customer_lifetime_days")
            )

        # Select numeric features for clustering
        feature_cols = ["total_spent", "transaction_count", "avg_transaction_value",
                       "product_diversity", "customer_lifetime_days", "purchase_frequency"]

        # Handle nulls and create feature vector
        for col_name in feature_cols:
            customer_features = customer_features.fillna(0, subset=[col_name])

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(customer_features)

        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(feature_df)
        scaled_df = scaler_model.transform(feature_df)

        # Apply K-means clustering
        kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=4, seed=42)
        model = kmeans.fit(scaled_df)
        clustered_df = model.transform(scaled_df)

        # Analyze clusters
        cluster_summary = clustered_df.groupBy("cluster") \
            .agg(
                count("customer_id").alias("customer_count"),
                avg("total_spent").alias("avg_total_spent"),
                avg("transaction_count").alias("avg_transaction_count"),
                avg("avg_transaction_value").alias("avg_transaction_value"),
                avg("product_diversity").alias("avg_product_diversity")
            ) \
            .orderBy("cluster")

        logger.info("✅ Customer clustering completed")
        return clustered_df, cluster_summary

    def time_series_analysis(self, df):
        """Perform time series analysis on sales data"""
        logger.info("Performing time series analysis...")

        # Daily aggregations
        daily_sales = df.groupBy("transaction_date") \
            .agg(
                sum("total_amount").alias("daily_revenue"),
                count("transaction_id").alias("daily_transactions"),
                countDistinct("customer_id").alias("daily_customers")
            ) \
            .orderBy("transaction_date")

        # Add moving averages
        window_7 = Window.orderBy("transaction_date").rowsBetween(-6, 0)
        window_30 = Window.orderBy("transaction_date").rowsBetween(-29, 0)

        daily_sales = daily_sales.withColumn("revenue_7day_ma", avg("daily_revenue").over(window_7)) \
                                 .withColumn("revenue_30day_ma", avg("daily_revenue").over(window_30))

        # Calculate growth rates
        window_lag = Window.orderBy("transaction_date")
        daily_sales = daily_sales.withColumn("prev_day_revenue", lag("daily_revenue", 1).over(window_lag)) \
                                 .withColumn("revenue_growth_rate",
                                           (col("daily_revenue") - col("prev_day_revenue")) / col("prev_day_revenue") * 100)

        # Weekly patterns
        weekly_patterns = df.withColumn("day_of_week", dayofweek("transaction_date")) \
                           .withColumn("day_name",
                                     when(col("day_of_week") == 1, "Sunday")
                                     .when(col("day_of_week") == 2, "Monday")
                                     .when(col("day_of_week") == 3, "Tuesday")
                                     .when(col("day_of_week") == 4, "Wednesday")
                                     .when(col("day_of_week") == 5, "Thursday")
                                     .when(col("day_of_week") == 6, "Friday")
                                     .when(col("day_of_week") == 7, "Saturday")) \
                           .groupBy("day_name", "day_of_week") \
                           .agg(
                               avg("total_amount").alias("avg_daily_revenue"),
                               count("transaction_id").alias("avg_daily_transactions")
                           ) \
                           .orderBy("day_of_week")

        logger.info("✅ Time series analysis completed")
        return daily_sales, weekly_patterns

    def product_affinity_analysis(self, df):
        """Analyze product purchasing patterns and affinity"""
        logger.info("Performing product affinity analysis...")

        # Market basket analysis - products bought together
        customer_products = df.groupBy("customer_id") \
            .agg(collect_list("product_name").alias("products"))

        # Product co-occurrence matrix (simplified)
        product_pairs = df.alias("a").join(df.alias("b"),
                                          (col("a.customer_id") == col("b.customer_id")) &
                                          (col("a.product_name") != col("b.product_name"))) \
                         .select(col("a.product_name").alias("product_a"),
                                col("b.product_name").alias("product_b")) \
                         .groupBy("product_a", "product_b") \
                         .count() \
                         .filter(col("count") >= 2) \
                         .orderBy(col("count").desc())

        # Product performance metrics
        product_metrics = df.groupBy("product_name") \
            .agg(
                sum("total_amount").alias("total_revenue"),
                count("transaction_id").alias("transaction_count"),
                countDistinct("customer_id").alias("unique_customers"),
                avg("total_amount").alias("avg_transaction_value"),
                sum("quantity").alias("total_quantity")
            ) \
            .withColumn("revenue_per_customer", col("total_revenue") / col("unique_customers")) \
            .orderBy(col("total_revenue").desc())

        logger.info("✅ Product affinity analysis completed")
        return product_pairs, product_metrics

    def statistical_summary(self, df):
        """Generate comprehensive statistical summary"""
        logger.info("Generating statistical summary...")

        # Overall business metrics
        total_revenue = df.agg(sum("total_amount")).collect()[0][0]
        total_transactions = df.count()
        unique_customers = df.select("customer_id").distinct().count()
        unique_products = df.select("product_name").distinct().count()
        date_range = df.agg(min("transaction_date"), max("transaction_date")).collect()[0]

        # Revenue distribution by percentiles
        revenue_percentiles = df.select("total_amount").approxQuantile("total_amount",
                                                                      [0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)

        # Customer value distribution
        customer_values = df.groupBy("customer_id").agg(sum("total_amount").alias("customer_value"))
        customer_percentiles = customer_values.select("customer_value").approxQuantile("customer_value",
                                                                                       [0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)

        summary = {
            'total_revenue': total_revenue,
            'total_transactions': total_transactions,
            'unique_customers': unique_customers,
            'unique_products': unique_products,
            'date_range': f"{date_range[0]} to {date_range[1]}",
            'avg_transaction_value': total_revenue / total_transactions,
            'avg_customer_value': total_revenue / unique_customers,
            'revenue_percentiles': {
                'p25': revenue_percentiles[0],
                'p50': revenue_percentiles[1],
                'p75': revenue_percentiles[2],
                'p90': revenue_percentiles[3],
                'p95': revenue_percentiles[4],
                'p99': revenue_percentiles[5]
            },
            'customer_value_percentiles': {
                'p25': customer_percentiles[0],
                'p50': customer_percentiles[1],
                'p75': customer_percentiles[2],
                'p90': customer_percentiles[3],
                'p95': customer_percentiles[4],
                'p99': customer_percentiles[5]
            }
        }

        logger.info("✅ Statistical summary generated")
        return summary

    def run_full_analysis(self, data_path, output_path):
        """Run complete advanced analytics suite"""
        logger.info("🚀 Starting Advanced Analytics Suite")
        start_time = datetime.now()

        try:
            # Load data
            df = self.load_data(data_path)

            # Run all analyses
            results = {}

            # 1. Statistical Summary
            results['stats'] = self.statistical_summary(df)

            # 2. Cohort Analysis
            results['cohorts'] = self.cohort_analysis(df)

            # 3. RFM Analysis
            results['rfm'] = self.rfm_analysis(df)

            # 4. Customer Clustering
            results['clusters'], results['cluster_summary'] = self.customer_clustering(df)

            # 5. Time Series Analysis
            results['daily_trends'], results['weekly_patterns'] = self.time_series_analysis(df)

            # 6. Product Affinity
            results['product_pairs'], results['product_metrics'] = self.product_affinity_analysis(df)

            # Save results
            for analysis_name, analysis_df in results.items():
                if hasattr(analysis_df, 'write'):  # It's a DataFrame
                    analysis_df.write.mode("overwrite").parquet(f"{output_path}/{analysis_name}")

            # Calculate runtime
            end_time = datetime.now()
            runtime = (end_time - start_time).total_seconds()

            logger.info(f"🎉 Advanced Analytics completed in {runtime:.2f} seconds")

            # Print comprehensive summary
            stats = results['stats']
            print("\n" + "="*80)
            print("📊 ADVANCED ANALYTICS SUMMARY")
            print("="*80)
            print(f"📈 Total Revenue: ${stats['total_revenue']:,.2f}")
            print(f"📊 Total Transactions: {stats['total_transactions']:,}")
            print(f"👥 Unique Customers: {stats['unique_customers']:,}")
            print(f"📦 Unique Products: {stats['unique_products']:,}")
            print(f"📅 Date Range: {stats['date_range']}")
            print(f"🛒 Avg Transaction: ${stats['avg_transaction_value']:.2f}")
            print(f"💎 Avg Customer Value: ${stats['avg_customer_value']:.2f}")
            print(f"\n💰 Revenue Distribution:")
            print(f"   25th percentile: ${stats['revenue_percentiles']['p25']:.2f}")
            print(f"   50th percentile: ${stats['revenue_percentiles']['p50']:.2f}")
            print(f"   75th percentile: ${stats['revenue_percentiles']['p75']:.2f}")
            print(f"   95th percentile: ${stats['revenue_percentiles']['p95']:.2f}")
            print(f"\n⏱️  Analysis Runtime: {runtime:.2f} seconds")
            print("="*80)

            return {
                'status': 'success',
                'runtime_seconds': runtime,
                'summary': stats,
                'output_path': output_path
            }

        except Exception as e:
            logger.error(f"❌ Advanced Analytics failed: {str(e)}")
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
    parser = argparse.ArgumentParser(description='Advanced Analytics for Big Data Sandbox')
    parser.add_argument('--data', required=True, help='Input data path')
    parser.add_argument('--output', required=True, help='Output path for results')

    args = parser.parse_args()

    analytics = AdvancedAnalytics()

    try:
        result = analytics.run_full_analysis(args.data, args.output)

        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)

    finally:
        analytics.cleanup()

if __name__ == "__main__":
    # Example usage when run directly
    analytics = AdvancedAnalytics()

    try:
        result = analytics.run_full_analysis(
            data_path="/data/sales_data.csv",
            output_path="s3a://processed/analytics"
        )

        print(f"\n🎯 Analytics result: {result['status']}")

    except Exception as e:
        print(f"❌ Analytics execution failed: {e}")
    finally:
        analytics.cleanup()