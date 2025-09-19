#!/usr/bin/env python3
"""
Sample Pipeline Script - Demonstrates complete ETL workflow
This script shows how to integrate all services in the Big Data Sandbox
"""

import sys
import time
import json
import requests
from datetime import datetime

def check_service(name, url, timeout=30):
    """Check if a service is available"""
    print(f"Checking {name}...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code in [200, 302]:
                print(f"✅ {name} is ready")
                return True
        except:
            pass
        time.sleep(2)

    print(f"❌ {name} is not ready after {timeout}s")
    return False

def trigger_airflow_dag(dag_id='sample_etl'):
    """Trigger an Airflow DAG"""
    print(f"\nTriggering Airflow DAG: {dag_id}")

    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic YWRtaW46YWRtaW4='  # admin:admin base64
    }
    data = {
        'conf': {},
        'dag_run_id': f'manual__{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    }

    try:
        response = requests.post(url, headers=headers, json=data, timeout=10)
        if response.status_code in [200, 201]:
            print(f"✅ DAG triggered successfully")
            return response.json()
        else:
            print(f"❌ Failed to trigger DAG: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error triggering DAG: {e}")
        return None

def check_dag_status(dag_id='sample_etl', dag_run_id=None):
    """Check the status of a DAG run"""
    if not dag_run_id:
        print("No DAG run ID provided")
        return None

    print(f"\nChecking DAG status: {dag_run_id}")

    url = f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    headers = {
        'Authorization': 'Basic YWRtaW46YWRtaW4='
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            state = data.get('state', 'unknown')
            print(f"DAG Run State: {state}")
            return data
        else:
            print(f"❌ Failed to get DAG status: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error checking DAG status: {e}")
        return None

def produce_sample_events():
    """Produce sample events to Kafka"""
    print("\nProducing sample events to Kafka...")

    try:
        import subprocess
        import os

        # Change to kafka producers directory
        producers_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'kafka', 'producers')

        # Run the event producer
        cmd = [
            sys.executable, 'event_producer.py',
            '--topic', 'user-events',
            '--event-type', 'user',
            '--count', '10',
            '--delay', '0.5'
        ]

        result = subprocess.run(cmd, cwd=producers_dir, capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            print("✅ Sample events produced successfully")
            return True
        else:
            print(f"❌ Failed to produce events: {result.stderr}")
            return False

    except Exception as e:
        print(f"❌ Error producing events: {e}")
        return False

def main():
    """Main pipeline execution"""
    print("🚀 Big Data Sandbox - Sample Pipeline")
    print("=" * 50)

    # Step 1: Check all services
    print("\n📋 Step 1: Verifying Services")
    services = {
        'Airflow': 'http://localhost:8080/health',
        'Spark': 'http://localhost:8081',
        'MinIO': 'http://localhost:9000/minio/health/live',
        'Kafka UI': 'http://localhost:9001',
        'Jupyter': 'http://localhost:8888'
    }

    all_ready = True
    for name, url in services.items():
        if not check_service(name, url):
            all_ready = False

    if not all_ready:
        print("\n❌ Some services are not ready. Please run 'docker compose up -d' and wait.")
        sys.exit(1)

    print("\n✅ All services are ready!")

    # Step 2: Produce sample events (optional)
    print("\n📋 Step 2: Producing Sample Events")
    produce_sample_events()

    # Step 3: Trigger ETL pipeline
    print("\n📋 Step 3: Triggering ETL Pipeline")
    dag_result = trigger_airflow_dag()

    if dag_result:
        dag_run_id = dag_result.get('dag_run_id')

        # Step 4: Monitor pipeline (basic check)
        print("\n📋 Step 4: Monitoring Pipeline")
        print("You can monitor the pipeline progress at:")
        print(f"📊 Airflow UI: http://localhost:8080/dags/sample_etl/graph")
        print(f"⚡ Spark UI: http://localhost:4040")
        print(f"💾 MinIO Console: http://localhost:9000")

        # Basic status check
        time.sleep(5)  # Give it a moment to start
        check_dag_status('sample_etl', dag_run_id)

        print("\n🎉 Pipeline initiated successfully!")
        print("\nNext steps:")
        print("1. Monitor the Airflow UI to see task progress")
        print("2. Check Spark UI for job execution details")
        print("3. Verify results in MinIO console")
        print("4. Explore Jupyter notebooks for data analysis")

    else:
        print("\n❌ Failed to trigger pipeline")
        print("Check the Airflow UI manually at: http://localhost:8080")
        sys.exit(1)

if __name__ == '__main__':
    main()