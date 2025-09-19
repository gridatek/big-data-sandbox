# Quickstart Guide - Complete ETL Pipeline

This guide walks you through building a complete ETL pipeline in under 10 minutes.

## Prerequisites
- All services running: `docker compose up -d`
- Wait 60 seconds for services to initialize

## Step 1: Verify Services (2 minutes)

Check all services are healthy:
```bash
# Run the verification script
./verify-services.sh
```

Expected output:
- ✅ Airflow: Ready
- ✅ Spark: Ready
- ✅ Kafka: Ready
- ✅ MinIO: Ready
- ✅ Jupyter: Ready

## Step 2: Upload Sample Data (1 minute)

```bash
# Create MinIO buckets
docker exec sandbox-minio mc mb local/raw-data local/processed local/models 2>/dev/null || true

# Upload sample data
docker exec sandbox-minio mc cp /data/sales_data.csv local/raw-data/
docker exec sandbox-minio mc cp /data/user_events.json local/raw-data/
docker exec sandbox-minio mc cp /data/iot_sensors.csv local/raw-data/

# Verify upload
docker exec sandbox-minio mc ls local/raw-data/
```

## Step 3: Trigger ETL Pipeline (1 minute)

### Option A: Via Airflow UI
1. Open http://localhost:8080 (admin/admin)
2. Find `sample_etl` DAG
3. Toggle it ON
4. Click "Trigger DAG"

### Option B: Via Command Line
```bash
# Trigger the ETL pipeline
curl -X POST "http://localhost:8080/api/v1/dags/sample_etl/dagRuns" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic YWRtaW46YWRtaW4=" \
  -d '{"conf":{}}'
```

## Step 4: Monitor Progress (2 minutes)

Watch the pipeline execute:

**Airflow UI**: http://localhost:8080
- DAG should show running tasks
- Click on task boxes to see logs

**Spark UI**: http://localhost:4040
- See active Spark jobs
- Monitor resource usage

**MinIO Console**: http://localhost:9000 (minioadmin/minioadmin)
- Watch processed files appear

## Step 5: Verify Results (1 minute)

```bash
# Check processed data
docker exec sandbox-minio mc ls local/processed/

# View sample processed data
docker exec sandbox-minio mc cat local/processed/sales_summary.json
```

## Step 6: Stream Real-time Events (3 minutes)

```bash
# Start event producer
cd kafka/producers
python event_producer.py &

# Monitor Kafka topics
docker exec sandbox-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --from-beginning
```

## What Just Happened?

1. **Data Ingestion**: Sample data uploaded to MinIO (S3-compatible storage)
2. **Orchestration**: Airflow triggered and managed the workflow
3. **Processing**: Spark read data from MinIO, processed it, wrote results back
4. **Streaming**: Kafka handled real-time event data
5. **Storage**: All results stored in MinIO for further analysis

## Next Steps

- **Advanced Analytics**: Open Jupyter at http://localhost:8888 (token: bigdata)
- **Real-time Processing**: Try `examples/streaming/`
- **Batch ETL**: Explore `examples/batch/`
- **Custom Pipelines**: Modify DAGs in `airflow/dags/`

## Troubleshooting

**Pipeline fails?**
- Check Airflow logs in the UI
- Verify MinIO buckets exist
- Ensure sample data is uploaded

**Services not responding?**
- Run `docker compose ps` to check status
- Restart failed services: `docker compose restart [service-name]`
- Check logs: `docker compose logs [service-name]`

**Need help?**
- Check main README.md
- Open an issue with error logs