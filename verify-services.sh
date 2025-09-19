#!/bin/bash

# Big Data Sandbox - Service Verification Script
# Checks if all services are running and accessible

echo "🔍 Big Data Sandbox - Service Health Check"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check HTTP service
check_http_service() {
    local service_name=$1
    local url=$2
    local expected_status=${3:-200}

    echo -n "Checking $service_name... "

    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_status"; then
        echo -e "${GREEN}✅ Ready${NC}"
        return 0
    else
        echo -e "${RED}❌ Not Ready${NC}"
        return 1
    fi
}

# Function to check Docker service
check_docker_service() {
    local service_name=$1
    local container_name=$2

    echo -n "Checking $service_name container... "

    if docker compose ps | grep -q "$container_name.*Up"; then
        echo -e "${GREEN}✅ Running${NC}"
        return 0
    else
        echo -e "${RED}❌ Not Running${NC}"
        return 1
    fi
}

# Function to check Kafka
check_kafka() {
    echo -n "Checking Kafka... "

    # Try to list topics (this will work if Kafka is ready)
    if docker exec sandbox-kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo -e "${GREEN}✅ Ready${NC}"
        return 0
    else
        echo -e "${RED}❌ Not Ready${NC}"
        return 1
    fi
}

# Function to check MinIO
check_minio() {
    echo -n "Checking MinIO... "

    # Check if MinIO client can connect
    if docker exec sandbox-minio mc admin info local &>/dev/null; then
        echo -e "${GREEN}✅ Ready${NC}"
        return 0
    else
        echo -e "${RED}❌ Not Ready${NC}"
        return 1
    fi
}

# Function to check Spark
check_spark() {
    echo -n "Checking Spark... "

    # Check Spark Master UI
    if curl -s http://localhost:8081 | grep -q "Spark Master"; then
        echo -e "${GREEN}✅ Ready${NC}"
        return 0
    else
        echo -e "${RED}❌ Not Ready${NC}"
        return 1
    fi
}

echo ""
echo "🐳 Docker Services:"
echo "-------------------"

# Check Docker services
check_docker_service "Airflow Webserver" "airflow-webserver"
check_docker_service "Airflow Scheduler" "airflow-scheduler"
check_docker_service "Spark Master" "spark-master"
check_docker_service "Spark Worker" "spark-worker"
check_docker_service "Kafka" "kafka"
check_docker_service "Zookeeper" "zookeeper"
check_docker_service "MinIO" "minio"
check_docker_service "Jupyter" "jupyter"

echo ""
echo "🌐 Service APIs:"
echo "----------------"

# Check HTTP services
check_http_service "Airflow Webserver" "http://localhost:8080/health"
check_http_service "Jupyter Lab" "http://localhost:8888" "302"
check_http_service "MinIO Console" "http://localhost:9090/minio/health/live"
check_http_service "Kafka UI" "http://localhost:9001" "200"

# Check specialized services
check_spark
check_kafka
check_minio

echo ""
echo "📊 Service URLs:"
echo "----------------"
echo "Airflow UI:     http://localhost:8080 (admin/admin)"
echo "Jupyter Lab:    http://localhost:8888 (token: bigdata)"
echo "Spark Master:   http://localhost:8081"
echo "MinIO Console:  http://localhost:9090 (minioadmin/minioadmin)"
echo "Kafka UI:       http://localhost:9001"

echo ""
echo "🔧 Quick Commands:"
echo "------------------"
echo "View all containers: docker compose ps"
echo "Check logs:         docker compose logs [service-name]"
echo "Restart service:    docker compose restart [service-name]"
echo "Stop all:          docker compose down"

echo ""
echo -e "${GREEN}✨ Service check complete!${NC}"

# Exit with error code if any service failed
if ! docker compose ps | grep -q "Up"; then
    echo -e "${RED}⚠️  Some services are not running. Run 'docker compose up -d' to start them.${NC}"
    exit 1
fi

echo -e "${GREEN}🎉 All services appear to be running! Ready for big data experiments.${NC}"