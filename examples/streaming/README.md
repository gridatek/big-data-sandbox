# Real-time Streaming Examples

This directory contains examples for real-time data processing with Kafka and Spark Streaming.

## Examples

### 1. Basic Kafka Streaming
- **File**: `kafka_streaming.py`
- **Description**: Demonstrates real-time event processing with Kafka
- **Features**: Event ingestion, filtering, and basic transformations

### 2. Spark Structured Streaming
- **File**: `spark_streaming.py`
- **Description**: Shows Spark Structured Streaming with Kafka integration
- **Features**: Real-time aggregations, windowing, and state management

### 3. Event Processing Pipeline
- **File**: `event_pipeline.py`
- **Description**: Complete real-time pipeline for user activity analysis
- **Features**: Event enrichment, anomaly detection, and real-time dashboards

## Quick Start

1. **Start the streaming producer**:
   ```bash
   cd kafka/producers
   python event_producer.py --continuous --topic user-events --rate 2
   ```

2. **Run the streaming consumer**:
   ```bash
   cd examples/streaming
   python spark_streaming.py
   ```

3. **Monitor the stream**:
   - Kafka UI: http://localhost:9001
   - Spark UI: http://localhost:4040

## Use Cases

- **Real-time Analytics**: Monitor user behavior as it happens
- **Fraud Detection**: Identify suspicious transactions in real-time
- **IoT Monitoring**: Process sensor data streams
- **Log Processing**: Real-time log analysis and alerting