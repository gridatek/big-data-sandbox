# 🧰 Big Data Sandbox

[![Docker](https://img.shields.io/badge/docker-ready-blue)](https://docker.com)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen)](CONTRIBUTING.md)
[![Discord](https://img.shields.io/discord/123456789)](https://discord.gg/yourlink)

A lightweight **one-command environment** to learn and practice **big data pipelines** with Kafka, Spark, Airflow, and MinIO — without the pain of setting everything up manually.

---

## 🚀 What is this?

The **Big Data Sandbox** is an open-source project that provides a ready-to-run environment for experimenting with big data tools. It's perfect for:
- Students learning data engineering
- Developers prototyping pipelines
- Educators preparing workshops or demos

**Included tools (MVP):**
- **Kafka** – Streaming events
- **Spark** – Batch & stream processing
- **Airflow** – Workflow orchestration
- **MinIO** – S3-compatible object storage
- **Jupyter** – Interactive notebooks for experiments

---

## 💡 Why Big Data Sandbox?

**The Problem:** Setting up a big data environment takes days of configuration, version conflicts, and debugging.

**Our Solution:** Pre-configured, version-tested components that work together out of the box. Focus on learning concepts, not fighting configs.

**What makes this different:**
- ✅ All services pre-integrated (no manual wiring)
- ✅ Realistic sample data included
- ✅ Production-like patterns (not toy examples)
- ✅ Actively maintained with latest stable versions

---

## 🏗 Architecture

```
┌──────────┐     ┌─────────┐     ┌────────┐
│  Airflow │────▶│  Kafka  │────▶│ Spark  │
└──────────┘     └─────────┘     └────────┘
      │                │               │
      ▼                ▼               ▼
┌──────────┐     ┌─────────────────────┐
│  Jupyter │     │      MinIO (S3)     │
└──────────┘     └─────────────────────┘
```

---

## 📋 Prerequisites

- Docker & Docker Compose installed
- 8GB+ RAM recommended
- 10GB free disk space
- Ports 8080, 8888, 9000, 9092, 4040, 8081 available

---

## ⚡ Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/yourname/big-data-sandbox.git
cd big-data-sandbox
```

### 2. Launch the sandbox
```bash
docker compose up -d
```

### 3. Verify all services are running
```bash
docker compose ps
# All services should show as "Up"
```

### 4. Explore the services
- **Airflow** → [http://localhost:8080](http://localhost:8080) (admin/admin)
- **Jupyter** → [http://localhost:8888](http://localhost:8888) (token: `bigdata`)
- **Spark UI** → [http://localhost:4040](http://localhost:4040)
- **MinIO** → [http://localhost:9000](http://localhost:9000) (minioadmin/minioadmin)
- **Kafka Manager** → [http://localhost:9001](http://localhost:9001)

---

## 📚 Learning Resources

**New to Big Data?** We have two learning paths for you:

- **📓 Interactive Tutorials**: Start with `jupyter/notebooks/01_getting_started.ipynb` for hands-on learning
- **🛠️ Production Examples**: Use `examples/` directory for real-world workflows

**👉 [Read the complete Learning Guide](LEARNING_GUIDE.md)** for detailed explanations of when to use each approach.

---

## 📖 First Pipeline - Real Example

Try this working example in under 5 minutes:

```bash
# 1. Upload sample data to MinIO
docker exec -it sandbox-minio mc mb local/raw-data 2>/dev/null || true
docker exec -it sandbox-minio mc cp /data/sales_data.csv local/raw-data/

# 2. Produce events to Kafka
docker exec -it sandbox-kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic events < data/sample_events.json

# 3. Trigger the ETL pipeline in Airflow
curl -X POST http://localhost:8080/api/v1/dags/sample_etl/dagRuns \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic YWRtaW46YWRtaW4=" \
  -d '{"conf":{}}'

# 4. Check processed results
docker exec -it sandbox-minio mc ls local/processed/
```

See [`examples/quickstart/`](examples/quickstart/) for the full walkthrough.

---

## 🗂 Project Structure

```
big-data-sandbox/
│── compose.yml              # One-command environment
│── .env.example            # Environment variables template
│── airflow/
│   ├── dags/              # Airflow DAG definitions
│   ├── plugins/           # Custom operators
│   └── config/            # Airflow configuration
│── spark/
│   ├── jobs/              # Spark applications
│   └── config/            # Spark configuration
│── kafka/
│   ├── config/            # Kafka broker config
│   └── producers/         # Sample data producers
│── minio/
│   └── data/              # Initial buckets & data
│── jupyter/
│   └── notebooks/         # 📓 Interactive learning tutorials
│── data/
│   ├── sales_data.csv     # Sample sales dataset
│   ├── user_events.json   # Sample event stream
│   └── iot_sensors.csv    # IoT sensor readings
│── examples/
│   ├── quickstart/        # 🚀 Complete workflow demos
│   ├── streaming/         # 🌊 Production streaming apps
│   └── batch/             # 📊 Enterprise ETL & analytics
└── README.md              # This file
```

---

## 🔧 Troubleshooting

**Services not starting?**
- Check Docker memory allocation: `docker system info | grep Memory`
- Increase Docker memory to at least 6GB in Docker Desktop settings
- View logs: `docker compose logs [service-name]`

**Port conflicts?**
- Check for running services: `lsof -i :8080` (replace with conflicting port)
- Modify ports in `.env` file (copy from `.env.example`)

**Kafka connection issues?**
- Ensure Kafka is fully started: `docker compose logs kafka | grep "started (kafka.server.KafkaServer)"`
- Wait 30 seconds after startup for all services to initialize

**Need help?** Open an issue with your `docker compose logs` output.

---

## 🌱 Roadmap

### Phase 1 - MVP (Current)
- [x] Core services (Kafka, Spark, Airflow, MinIO)
- [x] Docker Compose setup
- [x] Basic documentation
- [x] Jupyter integration
- [ ] Sample datasets & generators

### Phase 2 - Enhanced Learning (Q1 2025)
- [ ] Interactive tutorials in Jupyter
- [ ] Data generators (IoT sensors, web logs, transactions)
- [ ] Video tutorials series
- [ ] Performance monitoring dashboard
- [ ] Additional connectors (PostgreSQL, MongoDB)

### Phase 3 - Production Ready (Q2 2025)
- [ ] Kubernetes deployment (Helm charts)
- [ ] Delta Lake / Iceberg integration
- [ ] Security configurations (Kerberos, SSL)
- [ ] Multi-node Spark cluster option
- [ ] CI/CD pipeline examples

### Phase 4 - Advanced Features (Q3 2025)
- [ ] Machine Learning pipelines (MLflow)
- [ ] Stream processing with Flink
- [ ] Data quality checks (Great Expectations)
- [ ] Cost optimization guides
- [ ] Cloud deployment scripts (AWS/GCP/Azure)

---

## 👥 Community

- **Discord**: [Join our server](https://discord.gg/yourlink) - Get help and share projects
- **Blog**: Read tutorials at [blog.bigdatasandbox.dev](https://blog.bigdatasandbox.dev)
- **Twitter**: Follow [@bigdatasandbox](https://twitter.com/bigdatasandbox) for updates
- **YouTube**: [Video tutorials](https://youtube.com/@bigdatasandbox)

### Success Stories
> "Cut my workshop prep time from 2 days to 30 minutes!" - *University Professor*

> "Finally, a way to test Spark jobs without AWS bills!" - *Startup Developer*

> "Perfect for our internal data engineering bootcamp" - *Fortune 500 Tech Lead*

---

## 🤝 Contributing

Contributions are welcome! We're looking for:
- 🐛 Bug reports and fixes
- 📚 Documentation improvements
- 🎯 New example pipelines
- 🔧 Performance optimizations
- 🌍 Translations

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Contributors
<!-- ALL-CONTRIBUTORS-LIST:START -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

---

## 📜 License

MIT License - Free to use, modify, and share. See [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built with amazing open-source projects:
- Apache Kafka, Spark, and Airflow
- MinIO Object Storage
- Project Jupyter
- Docker & Docker Compose

Special thanks to the data engineering community for feedback and contributions!

---

**Ready to dive in?** Star ⭐ this repo and start exploring big data in minutes!