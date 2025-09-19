#!/usr/bin/env python3
"""
Basic Kafka Streaming Consumer
Demonstrates simple real-time event processing with Kafka
"""

import json
import time
import argparse
from datetime import datetime
from collections import defaultdict, deque
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaStreamProcessor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.metrics = {
            'total_events': 0,
            'events_by_type': defaultdict(int),
            'events_by_user': defaultdict(int),
            'recent_events': deque(maxlen=100),
            'revenue_total': 0.0,
            'start_time': datetime.now()
        }

    def connect(self, topics, group_id='streaming-consumer'):
        """Connect to Kafka with retry logic"""
        max_retries = 5
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    *topics,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                    consumer_timeout_ms=1000
                )
                logger.info(f"Connected to Kafka topics: {topics}")
                return

            except NoBrokersAvailable:
                logger.warning(f"Attempt {attempt + 1}: No brokers available. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

        raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

    def process_event(self, event):
        """Process a single event and update metrics"""
        if not event:
            return

        try:
            # Update basic metrics
            self.metrics['total_events'] += 1
            self.metrics['events_by_type'][event.get('event_type', 'unknown')] += 1
            self.metrics['events_by_user'][event.get('user_id', 'unknown')] += 1

            # Store recent event
            event['processed_at'] = datetime.now().isoformat()
            self.metrics['recent_events'].append(event)

            # Track revenue for purchase events
            if event.get('event_type') == 'purchase' and event.get('amount'):
                self.metrics['revenue_total'] += float(event['amount'])

            # Log interesting events
            if event.get('event_type') in ['purchase', 'login', 'search']:
                logger.info(f"📊 {event.get('event_type').upper()}: User {event.get('user_id')} - {event}")

        except Exception as e:
            logger.error(f"Error processing event: {e}")

    def detect_anomalies(self, event):
        """Simple anomaly detection"""
        anomalies = []

        try:
            user_id = event.get('user_id')
            if user_id:
                # High activity user detection
                user_events = self.metrics['events_by_user'][user_id]
                if user_events > 50:  # More than 50 events
                    anomalies.append({
                        'type': 'high_activity_user',
                        'user_id': user_id,
                        'event_count': user_events,
                        'detected_at': datetime.now().isoformat()
                    })

            # High value purchase detection
            if event.get('event_type') == 'purchase' and event.get('amount'):
                amount = float(event['amount'])
                if amount > 1000:  # High value purchase
                    anomalies.append({
                        'type': 'high_value_purchase',
                        'user_id': event.get('user_id'),
                        'amount': amount,
                        'order_id': event.get('order_id'),
                        'detected_at': datetime.now().isoformat()
                    })

            return anomalies

        except Exception as e:
            logger.error(f"Error in anomaly detection: {e}")
            return []

    def print_metrics(self):
        """Print current streaming metrics"""
        runtime = (datetime.now() - self.metrics['start_time']).total_seconds()
        events_per_second = self.metrics['total_events'] / runtime if runtime > 0 else 0

        print("\n" + "="*60)
        print("📊 REAL-TIME STREAMING METRICS")
        print("="*60)
        print(f"⏱️  Runtime: {runtime:.1f} seconds")
        print(f"📈 Total Events: {self.metrics['total_events']:,}")
        print(f"⚡ Events/Second: {events_per_second:.2f}")
        print(f"💰 Total Revenue: ${self.metrics['revenue_total']:,.2f}")

        print(f"\n📊 Events by Type:")
        for event_type, count in sorted(self.metrics['events_by_type'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / self.metrics['total_events']) * 100 if self.metrics['total_events'] > 0 else 0
            print(f"   {event_type:15} {count:6,} ({percentage:5.1f}%)")

        print(f"\n👥 Top Active Users:")
        top_users = sorted(self.metrics['events_by_user'].items(), key=lambda x: x[1], reverse=True)[:5]
        for user_id, count in top_users:
            print(f"   {user_id:15} {count:6,} events")

        if self.metrics['recent_events']:
            latest_event = self.metrics['recent_events'][-1]
            print(f"\n🔄 Latest Event: {latest_event.get('event_type')} by {latest_event.get('user_id')}")

        print("="*60)

    def run_streaming_analytics(self, topics, print_interval=10):
        """Run real-time streaming analytics"""
        logger.info("🌊 Starting Kafka streaming analytics...")

        try:
            self.connect(topics)

            last_print = time.time()
            message_count = 0

            print(f"\n🎯 Consuming from topics: {topics}")
            print(f"📊 Metrics will update every {print_interval} seconds")
            print("⏹️  Press Ctrl+C to stop\n")

            for message in self.consumer:
                try:
                    # Process the event
                    event = message.value
                    self.process_event(event)

                    # Check for anomalies
                    anomalies = self.detect_anomalies(event)
                    for anomaly in anomalies:
                        logger.warning(f"🚨 ANOMALY DETECTED: {anomaly}")

                    message_count += 1

                    # Print metrics periodically
                    current_time = time.time()
                    if current_time - last_print >= print_interval:
                        self.print_metrics()
                        last_print = current_time

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("🛑 Stopping streaming analytics...")

        except Exception as e:
            logger.error(f"❌ Streaming failed: {str(e)}")
            raise

        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")

    def export_metrics(self, output_file=None):
        """Export metrics to file"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"streaming_metrics_{timestamp}.json"

        try:
            # Convert deque to list for JSON serialization
            export_data = dict(self.metrics)
            export_data['recent_events'] = list(export_data['recent_events'])
            export_data['events_by_type'] = dict(export_data['events_by_type'])
            export_data['events_by_user'] = dict(export_data['events_by_user'])
            export_data['start_time'] = export_data['start_time'].isoformat()

            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)

            logger.info(f"📁 Metrics exported to: {output_file}")

        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description='Kafka Streaming Analytics')
    parser.add_argument('--topics', nargs='+', default=['user-events'], help='Kafka topics to consume')
    parser.add_argument('--brokers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--group-id', default='streaming-analytics', help='Consumer group ID')
    parser.add_argument('--interval', type=int, default=10, help='Metrics print interval (seconds)')
    parser.add_argument('--export', help='Export metrics to file on exit')

    args = parser.parse_args()

    processor = KafkaStreamProcessor(args.brokers)

    try:
        processor.run_streaming_analytics(args.topics, args.interval)
    finally:
        if args.export:
            processor.export_metrics(args.export)

if __name__ == "__main__":
    # Example usage when run directly
    processor = KafkaStreamProcessor()

    try:
        print("🌊 Starting Basic Kafka Streaming Demo")
        print("📋 Make sure to run the event producer first:")
        print("   cd kafka/producers")
        print("   python event_producer.py --continuous --topic user-events")
        print()

        processor.run_streaming_analytics(['user-events'], print_interval=15)

    except Exception as e:
        print(f"❌ Streaming failed: {e}")
    finally:
        processor.export_metrics()