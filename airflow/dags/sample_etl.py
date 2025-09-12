"""
Sample ETL Pipeline for Big Data Sandbox
This DAG demonstrates a typical ETL workflow:
1. Extract data from MinIO
2. Transform using Spark
3. Load results back to MinIO
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sample_etl',
    default_args=default_args,
    description='Sample ETL pipeline using Spark and MinIO',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'etl', 'spark'],
)

def check_data_quality(**context):
    """
    Data quality check function
    """
    logging.info("Performing data quality checks...")
    # In a real scenario, this would connect to MinIO and validate data
    # For demo purposes, we'll simulate a check
    import random

    quality_score = random.uniform(0.8, 1.0)
    logging.info(f"Data quality score: {quality_score:.2%}")

    if quality_score < 0.85:
        raise ValueError(f"Data quality below threshold: {quality_score:.2%}")

    # Push the score to XCom for downstream tasks
    context['task_instance'].xcom_push(key='quality_score', value=quality_score)
    return quality_score

def prepare_spark_config(**context):
    """
    Prepare Spark job configuration
    """
    quality_score = context['task_instance'].xcom_pull(key='quality_score')
    logging.info(f"Preparing Spark config with quality score: {quality_score}")

    config = {
        'input_path': 's3a://raw-data/sales_data.csv',
        'output_path': 's3a://processed/sales_summary',
        'quality_threshold': quality_score,
        'processing_date': context['ds']
    }

    context['task_instance'].xcom_push(key='spark_config', value=config)
    return config

def send_notification(**context):
    """
    Send completion notification
    """
    logging.info("Sending completion notification...")
    config = context['task_instance'].xcom_pull(key='spark_config')

    message = f"""
    ETL Pipeline Completed Successfully!

    Processing Date: {config['processing_date']}
    Input: {config['input_path']}
    Output: {config['output_path']}
    Quality Score: {config['quality_threshold']:.2%}
    """

    logging.info(message)
    # In production, this would send an email or Slack notification
    return "Notification sent"

# Task 1: Check data availability in MinIO
check_minio = BashOperator(
    task_id='check_minio_connection',
    bash_command="""
    echo "Checking MinIO connection..."
    # This would normally use mc (MinIO client) to check buckets
    echo "MinIO connection successful"
    """,
    dag=dag,
)

# Task 2: Data quality check
quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag,
)

# Task 3: Prepare Spark configuration
prepare_config = PythonOperator(
    task_id='prepare_spark_config',
    python_callable=prepare_spark_config,
    dag=dag,
)

# Task 4: Run Spark transformation job
spark_transform = BashOperator(
    task_id='spark_transformation',
    bash_command="""
    echo "Submitting Spark job..."
    # spark-submit --master spark://spark-master:7077 \
    #   --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    #   --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    #   --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    #   --conf spark.hadoop.fs.s3a.path.style.access=true \
    #   --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    #   /opt/spark-jobs/etl_job.py
    echo "Spark job completed"
    """,
    dag=dag,
)

# Task 5: Validate output
validate_output = BashOperator(
    task_id='validate_output',
    bash_command="""
    echo "Validating Spark output..."
    # mc ls local/processed/
    echo "Output validation successful"
    """,
    dag=dag,
)

# Task 6: Send notification
notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    trigger_rule='all_success',
    dag=dag,
)

# Task 7: Cleanup temporary files
cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
    echo "Cleaning up temporary files..."
    # rm -rf /tmp/spark-temp/*
    echo "Cleanup completed"
    """,
    trigger_rule='all_done',
    dag=dag,
)

# Define task dependencies
check_minio >> quality_check >> prepare_config >> spark_transform >> validate_output >> notify >> cleanup

# Add documentation
dag.doc_md = """
## Sample ETL Pipeline

This DAG demonstrates a complete ETL workflow using the Big Data Sandbox components:

### Pipeline Steps:
1. **Check MinIO Connection**: Verify access to object storage
2. **Data Quality Check**: Validate input data meets quality thresholds
3. **Prepare Config**: Generate Spark job configuration
4. **Spark Transform**: Execute the main transformation logic
5. **Validate Output**: Ensure output data was created successfully
6. **Send Notification**: Alert stakeholders of completion
7. **Cleanup**: Remove temporary files

### Trigger Manually:
```bash
curl -X POST http://localhost:8080/api/v1/dags/sample_etl/dagRuns \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic YWRtaW46YWRtaW4=" \
  -d '{"conf":{}}'
```

### Monitor Progress:
Visit http://localhost:8080 and check the DAG runs.
"""