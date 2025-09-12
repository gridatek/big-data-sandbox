"""
Kafka Event Producer for Big Data Sandbox
Generates and sends simulated events to Kafka topics
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random
from datetime import datetime
import argparse
import sys

class EventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        print(f"✅ Connected to Kafka at {bootstrap_servers}")

    def generate_user_event(self):
        """Generate a random user activity event"""
        events = ['page_view', 'click', 'purchase', 'add_to_cart', 'search', 'logout']
        pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/search']

        return {
            'event_id': f"EVT{random.randint(100000, 999999)}",
            'timestamp': datetime.now().isoformat(),
            'user_id': f"USER{random.randint(1, 1000):04d}",
            'session_id': f"SESSION{random.randint(1000, 9999)}",
            'event_type': random.choice(events),
            'page': random.choice(pages),
            'duration_ms': random.randint(100, 10000),
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'country': random.choice(['US', 'UK', 'DE', 'FR', 'JP'])
        }

    def generate_iot_event(self):
        """Generate a random IoT sensor event"""
        return {
            'sensor_id': f"SENSOR{random.randint(1, 50):03d}",
            'timestamp': datetime.now().isoformat(),
            'temperature': round(random.uniform(15, 35), 2),
            'humidity': round(random.uniform(30, 80), 2),
            'pressure': round(random.uniform(990, 1030), 2),
            'battery_level': round(random.uniform(0, 100), 1),
            'location': {
                'lat': round(random.uniform(-90, 90), 6),
                'lon': round(random.uniform(-180, 180), 6)
            },
            'status': random.choice(['active', 'idle', 'warning', 'error'])
        }

    def generate_transaction_event(self):
        """Generate a random transaction event"""
        return {
            'transaction_id': f"TXN{random.randint(100000, 999999)}",
            'timestamp': datetime.now().isoformat(),
            'customer_id': f"CUST{random.randint(1, 500):04d}",
            'amount': round(random.uniform(10, 1000), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bitcoin']),
            'merchant_id': f"MERCH{random.randint(1, 100):03d}",
            'category': random.choice(['retail', 'food', 'entertainment', 'services']),
            'fraud_score': round(random.uniform(0, 1), 3)
        }

    def send_event(self, topic, event, key=None):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            return record_metadata
        except KafkaError as e:
            print(f"❌ Error sending message: {e}")
            return None

    def produce_events(self, topic, event_type='user', count=100, delay=1):
        """Produce multiple events to a topic"""
        generators = {
            'user': self.generate_user_event,
            'iot': self.generate_iot_event,
            'transaction': self.generate_transaction_event
        }

        if event_type not in generators:
            print(f"❌ Unknown event type: {event_type}")
            return

        generator = generators[event_type]
        print(f"📨 Producing {count} {event_type} events to topic '{topic}'...")

        successful = 0
        failed = 0

        for i in range(count):
            event = generator()

            # Use appropriate key for partitioning
            if event_type == 'user':
                key = event.get('user_id')
            elif event_type == 'iot':
                key = event.get('sensor_id')
            else:
                key = event.get('customer_id')

            metadata = self.send_event(topic, event, key)

            if metadata:
                successful += 1
                print(f"✅ Event {i+1}/{count} sent - Partition: {metadata.partition}, Offset: {metadata.offset}")
            else:
                failed += 1
                print(f"❌ Event {i+1}/{count} failed")

            if delay > 0 and i < count - 1:
                time.sleep(delay)

        print(f"\n📊 Summary: {successful} successful, {failed} failed")
        self.producer.flush()

    def produce_continuous(self, topic, event_type='user', rate=1):
        """Continuously produce events at specified rate (events per second)"""
        generators = {
            'user': self.generate_user_event,
            'iot': self.generate_iot_event,
            'transaction': self.generate_transaction_event
        }

        if event_type not in generators:
            print(f"❌ Unknown event type: {event_type}")
            return

        generator = generators[event_type]
        delay = 1.0 / rate

        print(f"📨 Starting continuous production to topic '{topic}' at {rate} events/second")
        print("Press Ctrl+C to stop...")

        count = 0
        try:
            while True:
                event = generator()

                # Use appropriate key for partitioning
                if event_type == 'user':
                    key = event.get('user_id')
                elif event_type == 'iot':
                    key = event.get('sensor_id')
                else:
                    key = event.get('customer_id')

                metadata = self.send_event(topic, event, key)

                if metadata:
                    count += 1
                    print(f"✅ Event {count} sent - Type: {event_type}, Key: {key}")

                time.sleep(delay)

        except KeyboardInterrupt:
            print(f"\n🛑 Stopped. Total events sent: {count}")
        finally:
            self.close()

    def close(self):
        """Close the producer connection"""
        self.producer.flush()
        self.producer.close()
        print("👋 Producer closed")

def main():
    parser = argparse.ArgumentParser(description='Kafka Event Producer for Big Data Sandbox')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='events',
                        help='Kafka topic name (default: events)')
    parser.add_argument('--event-type', choices=['user', 'iot', 'transaction'],
                        default='user', help='Type of events to generate')
    parser.add_argument('--count', type=int, default=100,
                        help='Number of events to produce (default: 100)')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between events in seconds (default: 1.0)')
    parser.add_argument('--continuous', action='store_true',
                        help='Run in continuous mode')
    parser.add_argument('--rate', type=float, default=1.0,
                        help='Events per second in continuous mode (default: 1.0)')

    args = parser.parse_args()

    print("🚀 Big Data Sandbox - Kafka Event Producer")
    print("=" * 50)

    producer = EventProducer(args.bootstrap_servers)

    try:
        if args.continuous:
            producer.produce_continuous(args.topic, args.event_type, args.rate)
        else:
            producer.produce_events(args.topic, args.event_type, args.count, args.delay)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()