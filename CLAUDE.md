# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Big Data Sandbox is a Docker-based development environment for learning and experimenting with big data technologies. It provides pre-configured services that work together out of the box:

- **Kafka** (+ Zookeeper, Kafka UI) - Event streaming
- **Spark** (Master + Worker) - Batch and stream processing  
- **Airflow** (+ PostgreSQL) - Workflow orchestration
- **MinIO** - S3-compatible object storage
- **Jupyter** - Interactive notebooks

## Architecture

The system follows a typical big data pipeline pattern:
- Airflow orchestrates workflows and triggers Spark jobs
- Kafka handles real-time data streaming
- Spark processes data (reads from MinIO, writes results back)
- MinIO provides S3-compatible storage for data lake functionality
- All services communicate via the `bigdata-network` Docker network

## Essential Commands

### Environment Management
```bash
# Start all services
docker compose up -d

# Quick setup with validation
./quickstart.sh

# Stop all services
docker compose down

# View service status
docker compose ps

# View logs for specific service
docker compose logs -f [service-name]

# Restart specific service
docker compose restart [service-name]
```

### Service Access URLs
- Airflow: http://localhost:8080 (admin/admin)
- Jupyter: http://localhost:8888 (token: bigdata)
- Spark Master UI: http://localhost:8081
- Spark Application UI: http://localhost:4040
- MinIO Console: http://localhost:9090 (minioadmin/minioadmin)
- MinIO API: http://localhost:9000
- Kafka UI: http://localhost:9001

### Development Workflows

**Airflow DAG Development:**
- DAGs are stored in `./airflow/dags/`
- Changes are auto-detected by Airflow scheduler
- Test DAGs via Airflow UI or API

**Spark Job Development:**
- Spark applications go in `./spark/jobs/`
- Submit jobs through Airflow or directly via spark-submit
- Jobs have access to MinIO via S3A filesystem

**Data Management:**
- Sample data in `./data/` directory
- MinIO buckets: `raw-data`, `processed`, `models`
- Use MinIO client (mc) for data operations

## Key Components

### Airflow DAGs (`airflow/dags/`)
- `sample_etl.py`: Demonstrates complete ETL workflow with Spark integration
- DAGs use XCom for task communication and configuration passing

### Spark Jobs (`spark/jobs/`)
- `etl_job.py`: Sample ETL processing with MinIO integration
- Configured for S3A access to MinIO storage
- Includes data quality validation and multiple output formats

### Kafka Producers (`kafka/producers/`)
- `event_producer.py`: Sample event streaming producer

## Configuration

### Environment Variables
- Copy `.env.example` to `.env` for customization
- Key settings: ports, credentials, resource limits
- All services use consistent networking via `bigdata-net`

### Docker Compose Services
- Services are interdependent (startup order matters)
- Persistent volumes for data storage
- Health checks ensure service readiness

## Development Patterns

1. **ETL Pipelines**: Airflow → Spark → MinIO pattern
2. **Data Quality**: Built-in validation in sample jobs
3. **Configuration Management**: Environment variables and XCom
4. **Error Handling**: Retry logic and notification patterns

## Data Flow

1. Raw data uploaded to MinIO (`raw-data` bucket)
2. Airflow DAG triggers processing workflow
3. Spark job reads from MinIO, processes data
4. Results written back to MinIO (`processed` bucket)
5. Notification sent upon completion

## Troubleshooting

- Ensure 8GB+ RAM and required ports available
- Wait 30 seconds after startup for service initialization
- Check logs with `docker compose logs [service]`
- Restart individual services if needed
- Use `./quickstart.sh` for guided setup and validation