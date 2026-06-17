# ⚡ Data Sync Pipeline

> A production-ready, multi-source ETL pipeline with real-time monitoring dashboard — built with FastAPI, Prefect, PostgreSQL, and Streamlit, fully containerized with Docker.

![Architecture](./architecture.svg)

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Prefect](https://img.shields.io/badge/Prefect-3.x-0052FF?logo=prefect&logoColor=white)](https://prefect.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🚀 Features

- **Multi-source ingestion** — REST APIs, CSV files, and inbound Webhooks
- **ETL Pipeline** — Extract → Transform (normalize, deduplicate) → Load with SQLAlchemy upserts
- **Orchestration** — Prefect 3.x flows with structured logging and run tracking
- **Webhook server** — Async FastAPI endpoint that buffers events for the next pipeline run
- **Live dashboard** — Streamlit app with Plotly charts, KPI metrics, and auto-refresh
- **Fully containerized** — One `docker-compose up` to run everything

---

## 🏗️ Architecture

| Layer | Technology |
|---|---|
| Pipeline orchestration | Prefect 3.x |
| Webhook server | FastAPI + Uvicorn |
| Data extraction | httpx (API), pandas (CSV), SQLAlchemy (webhook buffer) |
| Transform | Python / Pydantic schemas |
| Storage | PostgreSQL 15 (Alpine) |
| Monitoring UI | Streamlit 1.33 + Plotly 5 |
| Containerization | Docker + Docker Compose |

---

## ⚙️ Quick Start

### Prerequisites
- Docker & Docker Compose

### Run locally

```bash
# Clone the repository
git clone https://github.com/farahioussama01/data-sync-pipeline.git
cd data-sync-pipeline

# Copy and configure environment
cp .env.example .env

# Build and start all services
docker-compose up --build
```

Once running, the following services are available:

| Service | Local | Description |
|---|---|---|
| Dashboard | `http://localhost:8501` | Streamlit monitoring UI |
| Webhook API | `http://localhost:8000` | FastAPI event receiver |
| API Docs | `http://localhost:8000/docs` | Interactive Swagger UI |
| PostgreSQL | `localhost:5432` | Relational database |

### Deploy to a VPS / cloud server

```bash
# On your server (Ubuntu/Debian)
git clone https://github.com/farahioussama01/data-sync-pipeline.git
cd data-sync-pipeline

# Set your production credentials
cp .env.example .env && nano .env

# Start in detached mode
docker-compose up -d --build
```

Then expose ports `8000` and `8501` via your reverse proxy (Nginx/Caddy).

### Run the pipeline manually

```bash
docker-compose exec webhook python -m flows.main_flow
```

### Send a test webhook event

```bash
curl -X POST http://<your-host>:8000/webhook/my-source \
  -H "Content-Type: application/json" \
  -d '{"title": "test event", "value": 42}'
```

---

## 📁 Project Structure

```
data-sync-pipeline/
├── flows/
│   ├── main_flow.py          # Prefect orchestration entry point
│   ├── extract/
│   │   ├── api_extractor.py  # REST API ingestion
│   │   ├── csv_extractor.py  # CSV file ingestion
│   │   └── webhook_extractor.py
│   ├── transform/
│   │   └── transformer.py    # Normalization & deduplication
│   └── load/
│       └── loader.py         # SQLAlchemy upsert logic
├── webhook/
│   └── server.py             # FastAPI webhook receiver
├── dashboard/
│   └── app.py                # Streamlit monitoring dashboard
├── db/
│   ├── database.py           # SQLAlchemy engine & session
│   └── models.py             # ORM models (3 tables)
├── models/
│   └── schemas.py            # Pydantic schemas
├── data/
│   └── sample.csv            # Sample CSV data source
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## 📊 Dashboard Pages

- **Overview** — KPI cards, donut chart (records by source), bar chart (runs over time), activity timeline
- **Run History** — Filterable table of all pipeline runs with status badges
- **Webhook Events** — Real-time log of inbound webhook events
- **Raw Records** — Paginated view of all synced records with source filter

---

## 🔧 Configuration

All configuration is handled via environment variables (`.env` file):

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://pipeline:pipeline@postgres:5432/datasync` | PostgreSQL connection string |
| `POSTGRES_USER` | `pipeline` | Database user |
| `POSTGRES_PASSWORD` | `pipeline` | Database password (change in production) |
| `POSTGRES_DB` | `datasync` | Database name |

---

## 🔄 CI/CD & Deployment

The pipeline is designed to run in any environment that supports Docker:

- **Local development** — `docker-compose up`
- **VPS / bare metal** — Docker Compose with a reverse proxy (Nginx, Caddy)
- **Cloud** — Compatible with Railway, Render, AWS EC2, DigitalOcean Droplets

---

## 📄 License

MIT
