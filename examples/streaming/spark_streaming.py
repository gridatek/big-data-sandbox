#!/usr/bin/env python3
"""
Spark Structured Streaming Example
Demonstrates real-time data processing with Kafka and Spark Streaming
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkStreamingProcessor:
    def __init__(self, app_name="Spark_Streaming"):
        """Initialize Spark session for streaming"""
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
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark Streaming session initialized: {self.spark.version}")

    def create_kafka_stream(self, topic, kafka_servers="kafka:9092"):
        """Create Kafka streaming DataFrame"""
        logger.info(f"Creating Kafka stream for topic: {topic}")

        stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        return stream_df

    def process_user_events(self, kafka_df):
        """Process user event stream with real-time analytics"""
        logger.info("Setting up user event processing pipeline...")

        # Define schema for user events
        user_event_schema = StructType([
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

        # Parse JSON from Kafka value
        parsed_df = kafka_df.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), user_event_schema).alias("data")
        ).select("kafka_timestamp", "data.*")

        # Add processing timestamp and clean data
        processed_df = parsed_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("event_timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
            .filter(col("user_id").isNotNull()) \
            .filter(col("event_type").isNotNull())

        return processed_df

    def real_time_aggregations(self, stream_df):
        """Create real-time aggregations with windowing"""
        logger.info("Setting up real-time aggregations...")

        # 1. Events per minute by type
        events_per_minute = stream_df \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute"),
                "event_type"
            ) \
            .count() \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "event_type",
                "count"
            )

        # 2. Active users per 5-minute window
        active_users = stream_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "5 minutes", "1 minute")
            ) \
            .agg(
                countDistinct("user_id").alias("active_users"),
                countDistinct("session_id").alias("active_sessions")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "active_users",
                "active_sessions"
            )

        # 3. Real-time revenue (for purchase events)
        revenue_stream = stream_df \
            .filter(col("event_type") == "purchase") \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute")
            ) \
            .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("purchase_count"),
                avg("amount").alias("avg_purchase_value")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "total_revenue",
                "purchase_count",
                "avg_purchase_value"
            )

        # 4. Device and location analytics
        device_analytics = stream_df \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window("event_timestamp", "5 minutes"),
                "device",
                "location"
            ) \
            .count() \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "device",
                "location",
                "count"
            )

        return {
            'events_per_minute': events_per_minute,
            'active_users': active_users,
            'revenue_stream': revenue_stream,
            'device_analytics': device_analytics
        }

    def anomaly_detection(self, stream_df):
        """Simple anomaly detection on streaming data"""
        logger.info("Setting up anomaly detection...")

        # Detect unusual patterns
        anomalies = stream_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute"),
                "user_id"
            ) \
            .count() \
            .filter(col("count") > 10) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "user_id",
                col("count").alias("events_per_minute")
            ) \
            .withColumn("anomaly_type", lit("high_activity"))

        return anomalies

    def write_to_console(self, stream_df, query_name, output_mode="append"):
        """Write stream to console for monitoring"""
        return stream_df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", False) \
            .queryName(query_name) \
            .trigger(processingTime='30 seconds')

    def write_to_parquet(self, stream_df, output_path, query_name, checkpoint_path):
        """Write stream to Parquet files"""
        return stream_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .queryName(query_name) \
            .trigger(processingTime='1 minute')

    def run_streaming_pipeline(self, topic="user-events", output_base_path="s3a://processed/streaming"):
        """Run the complete streaming pipeline"""
        logger.info("🚀 Starting Streaming Pipeline")

        try:
            # Create Kafka stream
            kafka_stream = self.create_kafka_stream(topic)

            # Process events
            processed_events = self.process_user_events(kafka_stream)

            # Create aggregations
            aggregations = self.real_time_aggregations(processed_events)

            # Set up anomaly detection
            anomalies = self.anomaly_detection(processed_events)

            # Start multiple streaming queries
            queries = []

            # 1. Console output for monitoring
            console_query = self.write_to_console(
                aggregations['events_per_minute'],
                "events_per_minute_console",
                "complete"
            ).start()
            queries.append(console_query)

            # 2. Write raw events to storage
            raw_events_query = self.write_to_parquet(
                processed_events,
                f"{output_base_path}/raw_events",
                "raw_events_parquet",
                f"{output_base_path}/checkpoints/raw_events"
            ).start()
            queries.append(raw_events_query)

            # 3. Write aggregations to storage
            aggregation_query = self.write_to_parquet(
                aggregations['active_users'],
                f"{output_base_path}/active_users",
                "active_users_parquet",
                f"{output_base_path}/checkpoints/active_users"
            ).start()
            queries.append(aggregation_query)

            # 4. Write anomalies to storage
            anomaly_query = self.write_to_parquet(
                anomalies,
                f"{output_base_path}/anomalies",
                "anomalies_parquet",
                f"{output_base_path}/checkpoints/anomalies"
            ).start()
            queries.append(anomaly_query)

            print("\n" + "="*60)
            print("🌊 STREAMING PIPELINE ACTIVE")
            print("="*60)
            print(f"📡 Source Topic: {topic}")
            print(f"💾 Output Path: {output_base_path}")
            print(f"⚡ Active Queries: {len(queries)}")
            print("\n📊 Real-time Analytics:")
            print("   • Events per minute by type")
            print("   • Active users and sessions")
            print("   • Revenue tracking")
            print("   • Device and location analytics")
            print("   • Anomaly detection")
            print("\n🔧 Monitoring:")
            print("   • Console output for events/minute")
            print("   • Parquet files for persistence")
            print("   • Watermarking for late data")
            print("\n⏹️  Press Ctrl+C to stop all queries")
            print("="*60)

            # Wait for all queries to terminate
            for query in queries:
                query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming pipeline...")
            for query in queries:
                query.stop()
            logger.info("✅ All streaming queries stopped")

        except Exception as e:
            logger.error(f"❌ Streaming pipeline failed: {str(e)}")
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
    parser = argparse.ArgumentParser(description='Spark Streaming for Big Data Sandbox')
    parser.add_argument('--topic', default='user-events', help='Kafka topic to consume')
    parser.add_argument('--output', default='s3a://processed/streaming', help='Output base path')

    args = parser.parse_args()

    processor = SparkStreamingProcessor()

    try:
        processor.run_streaming_pipeline(args.topic, args.output)
    finally:
        processor.cleanup()

if __name__ == "__main__":
    # Example usage when run directly
    processor = SparkStreamingProcessor()

    try:
        print("🌊 Starting Spark Streaming Demo")
        print("📋 Make sure to run the event producer first:")
        print("   cd kafka/producers")
        print("   python event_producer.py --continuous --topic user-events")
        print()

        processor.run_streaming_pipeline()

    except Exception as e:
        print(f"❌ Streaming failed: {e}")
    finally:
        processor.cleanup()