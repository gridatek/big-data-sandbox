#!/usr/bin/env python3
"""
Event Processing Pipeline - Complete Real-time Analytics System
Demonstrates advanced streaming patterns with event enrichment, anomaly detection, and dashboards
"""

import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from collections import defaultdict, deque
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventProcessingPipeline:
    def __init__(self, app_name="Event_Processing_Pipeline"):
        """Initialize Spark session for advanced streaming"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("spark://spark-master:7077") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages",
                   "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Event Processing Pipeline initialized: {self.spark.version}")

        # Initialize enrichment data
        self.user_profiles = self.load_user_profiles()
        self.product_catalog = self.load_product_catalog()

    def load_user_profiles(self):
        """Load user profile data for enrichment"""
        logger.info("Loading user profiles for enrichment...")

        # Sample user profile data
        user_data = [
            ("user_001", "Premium", "US", "2023-01-15", 1250.50),
            ("user_002", "Basic", "CA", "2023-06-20", 340.25),
            ("user_003", "Premium", "UK", "2022-11-10", 2100.75),
            ("user_004", "Standard", "DE", "2023-03-05", 825.00),
            ("user_005", "Premium", "FR", "2022-08-15", 1875.25),
        ]

        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("lifetime_value", DoubleType(), True)
        ])

        return self.spark.createDataFrame(user_data, schema)

    def load_product_catalog(self):
        """Load product catalog for enrichment"""
        logger.info("Loading product catalog for enrichment...")

        # Sample product catalog
        product_data = [
            ("prod_123", "Wireless Headphones", "Electronics", "Audio", 199.99, 4.5),
            ("prod_456", "Gaming Laptop", "Electronics", "Computing", 1299.99, 4.7),
            ("prod_789", "Smartphone", "Electronics", "Mobile", 699.99, 4.3),
            ("prod_101", "Coffee Maker", "Home", "Kitchen", 89.99, 4.1),
            ("prod_202", "Running Shoes", "Sports", "Footwear", 129.99, 4.4),
        ]

        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("rating", DoubleType(), True)
        ])

        return self.spark.createDataFrame(product_data, schema)

    def create_kafka_stream(self, topic, kafka_servers="kafka:9092"):
        """Create enriched Kafka streaming DataFrame"""
        logger.info(f"Creating enriched Kafka stream for topic: {topic}")

        # Base Kafka stream
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Define comprehensive event schema
        event_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("device", StringType(), True),
            StructField("location", StringType(), True),
            StructField("page", StringType(), True),
            StructField("element", StringType(), True),
            StructField("query", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])

        # Parse and clean events
        parsed_df = kafka_df.select(
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset"),
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("kafka_timestamp", "partition", "offset", "data.*")

        # Add processing metadata
        enriched_df = parsed_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
            .withColumn("event_hour", hour("event_timestamp")) \
            .withColumn("event_day_of_week", dayofweek("event_timestamp")) \
            .filter(col("user_id").isNotNull()) \
            .filter(col("event_type").isNotNull())

        return enriched_df

    def enrich_events(self, stream_df):
        """Enrich events with user and product data"""
        logger.info("Setting up event enrichment...")

        # Enrich with user profiles
        enriched_with_users = stream_df.join(
            broadcast(self.user_profiles),
            "user_id",
            "left"
        )

        # Enrich with product data for relevant events
        enriched_with_products = enriched_with_users.join(
            broadcast(self.product_catalog),
            "product_id",
            "left"
        )

        # Add derived enrichments
        fully_enriched = enriched_with_products \
            .withColumn("is_premium_user", col("tier") == "Premium") \
            .withColumn("is_high_value_event",
                       when(col("amount").isNotNull() & (col("amount") > 100), True)
                       .otherwise(False)) \
            .withColumn("event_value_score",
                       when(col("event_type") == "purchase", col("amount") * 10)
                       .when(col("event_type") == "add_to_cart", col("amount") * 5)
                       .when(col("event_type") == "page_view", 1.0)
                       .otherwise(0.5))

        logger.info("✅ Event enrichment pipeline configured")
        return fully_enriched

    def real_time_kpis(self, enriched_stream):
        """Calculate real-time business KPIs"""
        logger.info("Setting up real-time KPI calculations...")

        # 1. Real-time revenue tracking
        revenue_kpis = enriched_stream \
            .filter(col("event_type") == "purchase") \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(window("event_timestamp", "1 minute")) \
            .agg(
                sum("amount").alias("revenue"),
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_transaction_value"),
                countDistinct("user_id").alias("unique_buyers")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "revenue",
                "transaction_count",
                "avg_transaction_value",
                "unique_buyers"
            ) \
            .withColumn("kpi_type", lit("revenue"))

        # 2. User engagement metrics
        engagement_kpis = enriched_stream \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window("event_timestamp", "5 minutes"),
                "tier"
            ) \
            .agg(
                countDistinct("user_id").alias("active_users"),
                countDistinct("session_id").alias("active_sessions"),
                sum("event_value_score").alias("total_engagement_score")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "tier",
                "active_users",
                "active_sessions",
                "total_engagement_score"
            ) \
            .withColumn("kpi_type", lit("engagement"))

        # 3. Conversion funnel metrics
        conversion_events = enriched_stream \
            .filter(col("event_type").isin(["page_view", "add_to_cart", "purchase"])) \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "10 minutes"),
                "event_type"
            ) \
            .agg(
                countDistinct("user_id").alias("unique_users"),
                count("*").alias("event_count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "event_type",
                "unique_users",
                "event_count"
            ) \
            .withColumn("kpi_type", lit("conversion"))

        return {
            'revenue_kpis': revenue_kpis,
            'engagement_kpis': engagement_kpis,
            'conversion_kpis': conversion_events
        }

    def advanced_anomaly_detection(self, enriched_stream):
        """Advanced anomaly detection with multiple patterns"""
        logger.info("Setting up advanced anomaly detection...")

        # 1. High-frequency user activity
        high_activity_anomalies = enriched_stream \
            .withWatermark("event_timestamp", "3 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute"),
                "user_id"
            ) \
            .count() \
            .filter(col("count") > 15) \
            .select(
                col("window.start").alias("detection_time"),
                "user_id",
                col("count").alias("events_per_minute")
            ) \
            .withColumn("anomaly_type", lit("high_activity")) \
            .withColumn("severity",
                       when(col("events_per_minute") > 50, "critical")
                       .when(col("events_per_minute") > 30, "high")
                       .otherwise("medium"))

        # 2. Unusual purchase patterns
        purchase_anomalies = enriched_stream \
            .filter(col("event_type") == "purchase") \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "5 minutes"),
                "user_id"
            ) \
            .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("purchase_count")
            ) \
            .filter((col("total_spent") > 1000) | (col("purchase_count") > 5)) \
            .select(
                col("window.start").alias("detection_time"),
                "user_id",
                "total_spent",
                "purchase_count"
            ) \
            .withColumn("anomaly_type", lit("unusual_purchases")) \
            .withColumn("severity",
                       when(col("total_spent") > 5000, "critical")
                       .when(col("total_spent") > 2000, "high")
                       .otherwise("medium"))

        # 3. Geographic anomalies (rapid location changes)
        location_anomalies = enriched_stream \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window("event_timestamp", "10 minutes"),
                "user_id"
            ) \
            .agg(
                countDistinct("location").alias("location_count"),
                collect_set("location").alias("locations")
            ) \
            .filter(col("location_count") > 3) \
            .select(
                col("window.start").alias("detection_time"),
                "user_id",
                "location_count",
                "locations"
            ) \
            .withColumn("anomaly_type", lit("rapid_location_change")) \
            .withColumn("severity", lit("medium"))

        return {
            'high_activity': high_activity_anomalies,
            'purchase_patterns': purchase_anomalies,
            'location_patterns': location_anomalies
        }

    def user_journey_analytics(self, enriched_stream):
        """Track user journey and session analytics"""
        logger.info("Setting up user journey analytics...")

        # Session-based analytics
        session_analytics = enriched_stream \
            .withWatermark("event_timestamp", "30 minutes") \
            .groupBy(
                window("event_timestamp", "30 minutes"),
                "session_id",
                "user_id"
            ) \
            .agg(
                count("*").alias("session_events"),
                countDistinct("page").alias("pages_visited"),
                max("event_timestamp").alias("session_end"),
                min("event_timestamp").alias("session_start"),
                sum("event_value_score").alias("session_value"),
                collect_list("event_type").alias("event_sequence")
            ) \
            .withColumn("session_duration_minutes",
                       (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60) \
            .select(
                col("window.start").alias("window_start"),
                "session_id",
                "user_id",
                "session_events",
                "pages_visited",
                "session_duration_minutes",
                "session_value",
                "event_sequence"
            )

        # Customer lifecycle events
        lifecycle_events = enriched_stream \
            .filter(col("event_type").isin(["login", "purchase", "logout"])) \
            .withWatermark("event_timestamp", "1 hour") \
            .groupBy(
                window("event_timestamp", "1 hour"),
                "user_id",
                "tier"
            ) \
            .agg(
                collect_list("event_type").alias("lifecycle_events"),
                count("*").alias("lifecycle_event_count"),
                sum(when(col("event_type") == "purchase", col("amount")).otherwise(0)).alias("hourly_spend")
            ) \
            .select(
                col("window.start").alias("window_start"),
                "user_id",
                "tier",
                "lifecycle_events",
                "lifecycle_event_count",
                "hourly_spend"
            )

        return {
            'session_analytics': session_analytics,
            'lifecycle_events': lifecycle_events
        }

    def write_to_console_dashboard(self, stream_df, query_name):
        """Create console dashboard output"""
        return stream_df.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .queryName(query_name) \
            .trigger(processingTime='15 seconds')

    def write_to_storage(self, stream_df, output_path, query_name):
        """Write enriched stream to storage"""
        return stream_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{output_path}_checkpoint") \
            .queryName(query_name) \
            .trigger(processingTime='30 seconds')

    def run_event_pipeline(self, topic="user-events", output_base="s3a://processed/events"):
        """Run the complete event processing pipeline"""
        logger.info("🚀 Starting Advanced Event Processing Pipeline")

        try:
            # Create base stream
            raw_stream = self.create_kafka_stream(topic)

            # Enrich events
            enriched_stream = self.enrich_events(raw_stream)

            # Generate analytics
            kpis = self.real_time_kpis(enriched_stream)
            anomalies = self.advanced_anomaly_detection(enriched_stream)
            journeys = self.user_journey_analytics(enriched_stream)

            # Start streaming queries
            queries = []

            # 1. Real-time revenue dashboard
            revenue_query = self.write_to_console_dashboard(
                kpis['revenue_kpis'],
                "revenue_dashboard"
            ).start()
            queries.append(revenue_query)

            # 2. Anomaly alerts to console
            anomaly_query = self.write_to_console_dashboard(
                anomalies['high_activity'],
                "anomaly_alerts"
            ).start()
            queries.append(anomaly_query)

            # 3. Store enriched events
            enriched_query = self.write_to_storage(
                enriched_stream,
                f"{output_base}/enriched_events",
                "enriched_events_storage"
            ).start()
            queries.append(enriched_query)

            # 4. Store KPIs
            kpi_query = self.write_to_storage(
                kpis['engagement_kpis'],
                f"{output_base}/kpis",
                "kpi_storage"
            ).start()
            queries.append(kpi_query)

            # 5. Store anomalies
            anomaly_storage_query = self.write_to_storage(
                anomalies['purchase_patterns'],
                f"{output_base}/anomalies",
                "anomaly_storage"
            ).start()
            queries.append(anomaly_storage_query)

            # 6. Store user journeys
            journey_query = self.write_to_storage(
                journeys['session_analytics'],
                f"{output_base}/user_journeys",
                "journey_storage"
            ).start()
            queries.append(journey_query)

            print("\n" + "="*80)
            print("🌊 ADVANCED EVENT PROCESSING PIPELINE ACTIVE")
            print("="*80)
            print(f"📡 Source Topic: {topic}")
            print(f"💾 Output Base: {output_base}")
            print(f"⚡ Active Queries: {len(queries)}")
            print("\n🎯 Real-time Analytics:")
            print("   • Revenue & transaction KPIs")
            print("   • User engagement metrics")
            print("   • Conversion funnel tracking")
            print("   • Advanced anomaly detection")
            print("   • User journey analytics")
            print("   • Session behavior tracking")
            print("\n🔧 Features:")
            print("   • Event enrichment with user/product data")
            print("   • Multi-pattern anomaly detection")
            print("   • Watermarking for late data handling")
            print("   • Stateful session tracking")
            print("   • Real-time dashboards")
            print("\n📊 Dashboards:")
            print("   • Console: Revenue metrics")
            print("   • Console: Anomaly alerts")
            print("   • Storage: Historical analytics")
            print("\n⏹️  Press Ctrl+C to stop all queries")
            print("="*80)

            # Wait for termination
            for query in queries:
                query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("🛑 Stopping event processing pipeline...")
            for query in queries:
                query.stop()
            logger.info("✅ All streaming queries stopped")

        except Exception as e:
            logger.error(f"❌ Event processing pipeline failed: {str(e)}")
            for query in queries:
                if query.isActive:
                    query.stop()
            raise

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description='Advanced Event Processing Pipeline')
    parser.add_argument('--topic', default='user-events', help='Kafka topic to consume')
    parser.add_argument('--output', default='s3a://processed/events', help='Output base path')

    args = parser.parse_args()

    pipeline = EventProcessingPipeline()

    try:
        pipeline.run_event_pipeline(args.topic, args.output)
    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    # Example usage when run directly
    pipeline = EventProcessingPipeline()

    try:
        print("🌊 Starting Advanced Event Processing Pipeline")
        print("📋 Prerequisites:")
        print("   1. Start event producer: cd kafka/producers && python event_producer.py --continuous")
        print("   2. Ensure all services are running: ./verify-services.sh")
        print()

        pipeline.run_event_pipeline()

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
    finally:
        pipeline.cleanup()