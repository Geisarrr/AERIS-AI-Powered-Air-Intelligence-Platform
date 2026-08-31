# AERIS — AI-Powered Air Intelligence Platform

> **Intelligence behind the air you breathe.**

AERIS (*AI-Powered Air Intelligence Platform*) adalah platform berbasis **Artificial Intelligence** yang dirancang untuk melakukan **monitoring, analisis, dan prediksi kualitas udara** secara terintegrasi.

AERIS menggabungkan data kualitas udara, teknologi geospasial, data processing pipeline, dan Machine Learning untuk menghasilkan informasi kualitas udara yang lebih mudah dipahami serta mendukung pengambilan keputusan berbasis data.

---

## 🚧 Project Status

**Current Phase: Infrastructure & Application Foundation**

AERIS saat ini telah memiliki fondasi utama aplikasi, meliputi:

* [x] Monorepo project structure
* [x] FastAPI backend
* [x] Next.js frontend
* [x] Docker & Docker Compose
* [x] PostgreSQL
* [x] PostGIS
* [x] Redis
* [x] Backend & frontend containerization
* [x] Environment configuration
* [x] Service health checks
* [x] Backend health endpoint
* [ ] Database integration
* [ ] Air quality data pipeline
* [ ] Machine Learning pipeline
* [ ] Air quality prediction
* [ ] Anomaly detection
* [ ] Interactive dashboard
* [ ] Geospatial visualization
* [ ] Alert & notification system

> **Note:** PostgreSQL/PostGIS and Redis are currently prepared as infrastructure services. Active integration with the application layer is still under development.

---

## 🎯 Project Goals

AERIS is being developed to provide a centralized platform for:

1. **Air Quality Monitoring**
   Monitor air quality data across different locations.

2. **Air Quality Analysis**
   Analyze historical and real-time air quality data.

3. **Air Quality Prediction**
   Predict future air quality conditions using Machine Learning.

4. **Anomaly Detection**
   Detect unusual or abnormal air quality patterns.

5. **Geospatial Intelligence**
   Visualize air quality information based on geographical locations.

6. **Explainable AI**
   Provide insights into the factors influencing air quality predictions.

---

## 🏗️ System Architecture

The current architecture consists of four primary services:

```text
                         ┌─────────────────────┐
                         │       Browser       │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Next.js Frontend  │
                         │      Port 3000      │
                         └──────────┬──────────┘
                                    │
                                    │ HTTP / REST API
                                    ▼
                         ┌─────────────────────┐
                         │   FastAPI Backend   │
                         │      Port 8000      │
                         └──────┬────────┬─────┘
                                │        │
                     ┌──────────┘        └──────────┐
                     ▼                              ▼
          ┌───────────────────┐          ┌───────────────────┐
          │ PostgreSQL/PostGIS│          │       Redis        │
          │      Port 5432    │          │      Port 6379     │
          └───────────────────┘          └───────────────────┘
```

The application is orchestrated using **Docker Compose**.

---

## Technology Stack

### Frontend

| Technology   | Purpose                   |
| ------------ | ------------------------- |
| Next.js      | Web application framework |
| React        | UI library                |
| TypeScript   | Type-safe development     |
| Tailwind CSS | Styling                   |
| ESLint       | Code quality & linting    |

### Backend

| Technology  | Purpose                 |
| ----------- | ----------------------- |
| FastAPI     | REST API framework      |
| Python 3.11 | Backend language        |
| Uvicorn     | ASGI server             |
| Pydantic    | Data validation         |
| SQLAlchemy  | Database ORM            |
| Alembic     | Database migration      |
| psycopg2    | PostgreSQL connectivity |
| GeoAlchemy2 | PostGIS integration     |

### Database & Infrastructure

| Technology     | Purpose                         |
| -------------- | ------------------------------- |
| PostgreSQL     | Relational database             |
| PostGIS        | Geospatial data support         |
| Redis          | Cache & asynchronous processing |
| Docker         | Containerization                |
| Docker Compose | Multi-container orchestration   |

### Machine Learning

Planned ML components:

| Technology       | Purpose                     |
| ---------------- | --------------------------- |
| XGBoost          | Air quality prediction      |
| Isolation Forest | Anomaly detection           |
| SHAP             | Explainable AI              |
| MLflow           | Experiment & model tracking |

---

## 📁 Project Structure

```text
AERIS-AI-Powered-Air-Intelligence-Platform/
│
├── backend/
│   ├── app/
│   │   ├── api/
│   │   ├── __init__.py
│   │   └── main.py
│   │
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── .dockerignore
│   └── .env.example
│
├── frontend/
│   ├── app/
│   │   ├── globals.css
│   │   ├── layout.tsx
│   │   └── page.tsx
│   │
│   ├── public/
│   ├── Dockerfile
│   ├── package.json
│   ├── next.config.ts
│   ├── tsconfig.json
│   └── .dockerignore
│
├── data/
│   ├── raw/
│   └── processed/
│
├── ml/
│   └── models/
│
├── pipeline/
│
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

## ⚙️ Backend

The backend is built using **FastAPI** and serves as the central API layer for AERIS.

### Current Endpoints

#### `GET /`

Returns basic API information.

```json
{
  "message": "AERIS API is running",
  "version": "0.1.0"
}
```

#### `GET /health`

Returns the current API health status.

```json
{
  "status": "healthy"
}
```

### API Documentation

FastAPI automatically provides interactive API documentation.

```text
http://localhost:8000/docs
```

Alternative documentation:

```text
http://localhost:8000/redoc
```

---

## 🖥️ Frontend

The frontend is built using **Next.js, React, and TypeScript**.

The current frontend is still in the initial application scaffold stage.

Planned components include:

* Air Quality Dashboard
* Real-time air quality indicators
* Historical air quality charts
* Interactive maps
* Monitoring stations
* Air quality forecasting
* Anomaly visualization
* Environmental alerts
* AI-generated insights

The frontend API configuration is prepared through:

```text
NEXT_PUBLIC_API_URL
```

The actual frontend-to-backend API integration is still under development.

---

## 🗄️ Database & Infrastructure

AERIS uses **PostgreSQL with PostGIS** as the primary database infrastructure.

### PostgreSQL + PostGIS

PostGIS provides geospatial capabilities required for future features such as:

* Monitoring station coordinates
* Geographic regions
* Spatial queries
* Location-based air quality analysis
* Map visualization

The database is currently provisioned through Docker Compose.

Database schema and Alembic migrations will be implemented in the next development stage.

### Redis

Redis is included as an infrastructure service and is planned to support:

* API caching
* Air quality data caching
* Background processing
* Machine Learning queues
* Temporary application data
* Asynchronous workflows

Redis infrastructure is already available, while active application integration is still under development.

> **Security:** Database credentials, connection strings, API keys, tokens, and other sensitive configuration must not be committed to the repository.

---

## 🔐 Environment Configuration

AERIS uses environment variables for application configuration.

Create a local environment file from the provided example:

```bash
cp .env.example .env
```

The `.env.example` file should contain placeholders rather than real credentials:

```env
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

DATABASE_URL=
REDIS_URL=
```

> **Never commit `.env` files containing credentials or secrets to the repository.**

---

## 🐳 Docker Architecture

AERIS uses Docker Compose to orchestrate four services:

```text
┌──────────────────────────────────────────────┐
│                Docker Compose                │
│                                              │
│  ┌────────────┐      ┌────────────┐          │
│  │ PostgreSQL │      │   Redis    │          │
│  │  + PostGIS │      │            │          │
│  └─────┬──────┘      └─────┬──────┘          │
│        │                    │                 │
│        └──────────┬─────────┘                 │
│                   ▼                           │
│             ┌──────────┐                     │
│             │ FastAPI  │                     │
│             │ Backend  │                     │
│             └────┬─────┘                     │
│                  │                           │
│                  ▼                           │
│             ┌──────────┐                     │
│             │ Next.js  │                     │
│             │ Frontend │                     │
│             └──────────┘                     │
│                                              │
└──────────────────────────────────────────────┘
```

The backend is configured to wait for required infrastructure services to become healthy before starting.

---

## 🔄 Application Flow

The current application flow is:

```text
Browser
   │
   ▼
Next.js Frontend
   │
   │ HTTP / REST API
   ▼
FastAPI Backend
   │
   ├── PostgreSQL + PostGIS
   │
   └── Redis
```

The future intelligent data flow is planned as:

```text
Data Sources
     │
     ▼
Data Collection
     │
     ▼
Data Processing
     │
     ▼
PostgreSQL / Data Storage
     │
     ▼
Machine Learning
     │
     ├── XGBoost
     ├── Isolation Forest
     └── SHAP
     │
     ▼
FastAPI
     │
     ▼
Next.js Dashboard
```

---

## 🤖 Machine Learning

The Machine Learning layer will become one of the core components of AERIS.

### Air Quality Prediction

**XGBoost** is planned to be used for predicting future air quality conditions based on historical and environmental features.

Potential input features include:

```text
PM2.5
PM10
CO
NO2
SO2
O3
Temperature
Humidity
Wind Speed
Weather Conditions
Location
Historical Air Quality
```

### Anomaly Detection

**Isolation Forest** is planned for detecting unusual air quality patterns, including:

* Sudden pollution spikes
* Unusual environmental conditions
* Unexpected changes in pollutant concentration
* Potential sensor anomalies

### Explainable AI

**SHAP** will be used to provide explanations behind Machine Learning predictions.

The goal is to allow users to understand not only:

> **What does AERIS predict?**

but also:

> **Why did AERIS make that prediction?**

---

## 🔄 Data Pipeline

The planned data processing pipeline is:

```text
Data Sources
     │
     ▼
Data Collection
     │
     ▼
Raw Dataset
data/raw/
     │
     ▼
Data Cleaning
     │
     ▼
Feature Engineering
     │
     ▼
Processed Dataset
data/processed/
     │
     ▼
Model Training
     │
     ▼
Model Storage
ml/models/
     │
     ▼
FastAPI Inference
     │
     ▼
AERIS Dashboard
```

Potential future data sources include:

* Air quality monitoring stations
* Government datasets
* Weather APIs
* Satellite data
* Environmental datasets

The data pipeline is currently in the planning and development stage.

---

## 🚀 Getting Started

### Prerequisites

Make sure the following tools are installed:

* Docker
* Docker Compose
* Git

### 1. Clone Repository

```bash
git clone <repository-url>
cd AERIS-AI-Powered-Air-Intelligence-Platform
```

### 2. Configure Environment

Create the local environment file:

```bash
cp .env.example .env
```

Then configure the required environment variables locally.

### 3. Start the Application

Build and start all services:

```bash
docker compose up --build
```

Or run in detached mode:

```bash
docker compose up --build -d
```

### 4. Access the Application

**Frontend**

```text
http://localhost:3000
```

**Backend**

```text
http://localhost:8000
```

**FastAPI Swagger**

```text
http://localhost:8000/docs
```

**FastAPI ReDoc**

```text
http://localhost:8000/redoc
```

---

## 🔍 Development Status

### Infrastructure

* [x] Docker Compose
* [x] PostgreSQL
* [x] PostGIS
* [x] Redis
* [x] Backend container
* [x] Frontend container
* [x] Service health checks
* [x] Environment configuration

### Backend

* [x] FastAPI setup
* [x] Uvicorn setup
* [x] Root endpoint
* [x] Health endpoint
* [ ] Database connection
* [ ] Database models
* [ ] Alembic migrations
* [ ] Air quality API
* [ ] Forecast API
* [ ] Anomaly API
* [ ] Authentication

### Frontend

* [x] Next.js setup
* [x] TypeScript
* [x] Tailwind CSS
* [ ] AERIS UI
* [ ] Dashboard
* [ ] API integration
* [ ] Charts
* [ ] Interactive map
* [ ] Forecast visualization
* [ ] Alert system

### Data & Machine Learning

* [ ] Data ingestion
* [ ] Data cleaning
* [ ] Feature engineering
* [ ] Dataset generation
* [ ] XGBoost model
* [ ] Isolation Forest model
* [ ] SHAP integration
* [ ] MLflow
* [ ] Model serving
* [ ] Automated prediction pipeline

---

## 🗺️ Development Roadmap

```text
Phase 1 — Foundation
    │
    ├── Project structure
    ├── Docker
    ├── Backend
    ├── Frontend
    ├── PostgreSQL/PostGIS
    └── Redis
         │
         ▼
Phase 2 — Backend & Database
    │
    ├── Database schema
    ├── Alembic migrations
    ├── SQLAlchemy models
    ├── Air quality API
    └── API integration
         │
         ▼
Phase 3 — Data Pipeline
    │
    ├── Data ingestion
    ├── Data cleaning
    ├── Feature engineering
    └── Data storage
         │
         ▼
Phase 4 — Machine Learning
    │
    ├── XGBoost prediction
    ├── Isolation Forest
    ├── SHAP
    └── MLflow
         │
         ▼
Phase 5 — AERIS Dashboard
    │
    ├── Real-time monitoring
    ├── Interactive map
    ├── Analytics
    ├── Forecast
    ├── Anomaly detection
    └── Alerts
         │
         ▼
Phase 6 — Production
    │
    ├── Authentication
    ├── CI/CD
    ├── Monitoring
    ├── Security hardening
    └── Cloud deployment
```

---

## 📌 Current Milestone

AERIS has successfully moved beyond the initial project setup and now has a **containerized full-stack foundation**.

The project is currently transitioning from:

```text
Infrastructure
      ↓
Functional Backend
      ↓
Database Integration
      ↓
Data Pipeline
      ↓
Machine Learning
      ↓
Intelligence Dashboard
```

The next major milestone is to establish the **PostgreSQL/PostGIS database layer and backend API**, followed by implementation of the data ingestion and Machine Learning pipeline.

---

## 🌱 Vision

AERIS is not intended to be just an air quality dashboard.

The long-term vision is to build an **AI-powered environmental intelligence platform** capable of transforming raw environmental data into actionable intelligence:

```text
Data
  ↓
Information
  ↓
Analysis
  ↓
Prediction
  ↓
Explanation
  ↓
Action
```

> **AERIS — Intelligence behind the air you breathe.**
