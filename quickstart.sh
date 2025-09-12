#!/bin/bash

# Big Data Sandbox - Quick Start Script
# This script sets up and launches the entire environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ASCII Banner
echo -e "${BLUE}"
cat << "EOF"
╔══════════════════════════════════════════╗
║     BIG DATA SANDBOX - QUICK START       ║
║         Learn Big Data, Fast!            ║
╚══════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Function to print colored messages
print_message() {
    echo -e "${2}${1}${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    print_message "📋 Checking prerequisites..." "$YELLOW"

    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_message "❌ Docker is not installed. Please install Docker first." "$RED"
        exit 1
    fi

    # Check Docker Compose
    if ! docker compose version &> /dev/null; then
        print_message "❌ Docker Compose is not available. Please install Docker with Compose plugin." "$RED"
        exit 1
    fi

    # Check Docker daemon
    if ! docker info &> /dev/null; then
        print_message "❌ Docker daemon is not running. Please start Docker." "$RED"
        exit 1
    fi

    print_message "✅ All prerequisites met!" "$GREEN"
}

# Function to check available resources
check_resources() {
    print_message "🔍 Checking system resources..." "$YELLOW"

    # Check available memory
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        TOTAL_MEM=$(sysctl -n hw.memsize | awk '{print $1/1024/1024/1024}')
    else
        # Linux
        TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
    fi

    if (( $(echo "$TOTAL_MEM < 8" | bc -l) )); then
        print_message "⚠️  Warning: Less than 8GB RAM available. Services may run slowly." "$YELLOW"
        read -p "Continue anyway? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        print_message "✅ Sufficient memory available (${TOTAL_MEM}GB)" "$GREEN"
    fi
}

# Function to create necessary directories
create_directories() {
    print_message "📁 Creating project directories..." "$YELLOW"

    mkdir -p airflow/dags airflow/plugins
    mkdir -p spark/jobs spark/config
    mkdir -p kafka/config kafka/producers
    mkdir -p jupyter/notebooks
    mkdir -p data logs
    mkdir -p minio/data

    print_message "✅ Directories created" "$GREEN"
}

# Function to setup environment
setup_environment() {
    print_message "🔧 Setting up environment..." "$YELLOW"

    # Create .env file if it doesn't exist
    if [ ! -f .env ]; then
        if [ -f .env.example ]; then
            cp .env.example .env
            print_message "✅ Created .env file from template" "$GREEN"
        else
            print_message "⚠️  No .env.example found, using defaults" "$YELLOW"
        fi
    else
        print_message "✅ Using existing .env file" "$GREEN"
    fi
}

# Function to pull Docker images
pull_images() {
    print_message "🐳 Pulling Docker images (this may take a few minutes)..." "$YELLOW"
    docker compose pull
    print_message "✅ Docker images ready" "$GREEN"
}

# Function to start services
start_services() {
    print_message "🚀 Starting all services..." "$YELLOW"

    docker compose up -d

    print_message "⏳ Waiting for services to initialize (30 seconds)..." "$YELLOW"
    sleep 30

    print_message "✅ All services started!" "$GREEN"
}

# Function to verify services
verify_services() {
    print_message "🔍 Verifying service health..." "$YELLOW"

    # Check if containers are running
    RUNNING=$(docker compose ps --services --filter "status=running" | wc -l)
    EXPECTED=10  # Adjust based on your docker-compose.yml

    if [ "$RUNNING" -lt "$EXPECTED" ]; then
        print_message "⚠️  Some services may not be running properly" "$YELLOW"
        docker compose ps
    else
        print_message "✅ All services are healthy!" "$GREEN"
    fi
}

# Function to display access information
display_info() {
    echo
    print_message "🎉 BIG DATA SANDBOX IS READY!" "$GREEN"
    echo
    print_message "📋 Service URLs:" "$BLUE"
    echo "  • Airflow:      http://localhost:8080 (admin/admin)"
    echo "  • Jupyter:      http://localhost:8888 (token: bigdata)"
    echo "  • Spark UI:     http://localhost:4040"
    echo "  • MinIO:        http://localhost:9000 (minioadmin/minioadmin)"
    echo "  • Kafka UI:     http://localhost:9001"
    echo
    print_message "📚 Quick Start Commands:" "$BLUE"
    echo "  • View logs:        docker compose logs -f [service]"
    echo "  • Stop all:         docker compose down"
    echo "  • Restart service:  docker compose restart [service]"
    echo "  • Open Jupyter:     open http://localhost:8888"
    echo
    print_message "📖 Next Steps:" "$BLUE"
    echo "  1. Open Jupyter notebook for tutorials"
    echo "  2. Try the sample ETL pipeline in Airflow"
    echo "  3. Explore data in MinIO"
    echo
}

# Function to run sample pipeline
run_sample() {
    read -p "Would you like to run a sample data pipeline? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_message "🏃 Running sample pipeline..." "$YELLOW"

        # Upload sample data to MinIO
        docker exec sandbox-minio mc mb local/raw-data 2>/dev/null || true
        docker exec sandbox-minio mc cp /sample-data/sales_data.csv local/raw-data/ 2>/dev/null || true

        # Trigger Airflow DAG
        curl -X POST http://localhost:8080/api/v1/dags/sample_etl/dagRuns \
            -H "Content-Type: application/json" \
            -H "Authorization: Basic YWRtaW46YWRtaW4=" \
            -d '{"conf":{}}' 2>/dev/null || true

        print_message "✅ Sample pipeline triggered! Check Airflow UI for progress." "$GREEN"
    fi
}

# Main execution
main() {
    print_message "Starting Big Data Sandbox setup..." "$BLUE"

    check_prerequisites
    check_resources
    create_directories
    setup_environment
    pull_images
    start_services
    verify_services
    display_info
    run_sample

    print_message "✨ Setup complete! Happy learning! ✨" "$GREEN"
}

# Handle errors
trap 'print_message "❌ An error occurred. Check the logs with: docker compose logs" "$RED"' ERR

# Handle Ctrl+C
trap 'print_message "\n⚠️  Setup interrupted. Run ./quickstart.sh to continue." "$YELLOW"; exit 1' INT

# Run main function
main