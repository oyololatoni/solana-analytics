# Solana Analytics Pipeline

A comprehensive data pipeline to ingest, normalize, and analyze Solana swap events using Helius enhanced transactions.

## Table of Contents
* [Overview](#overview)
* [File Structure](#file-structure)
* [Getting Started](#getting-started)
* [Development](#development)
* [Deployment](#deployment)

---

## Overview

This project ingests swap transactions for specific tracked tokens from the Solana blockchain via Helius webhooks. It normalizes these events into a structured Postgres database (Neon) and provides an API for querying analytics (volume, unique makers, swap counts).

**Key Features:**
*   **Real-time Ingestion**: Webhook receiver (`/webhooks/helius`) for live data.
*   **Historical Backfill**: Script to sync past data (`backfill_solana.py`).
*   **Data Integrity**: Idempotency checks, broad swap leg detection, and granular ignore reason tracking.
*   **Observability**: Structured metrics API (`/metrics/ingestion-stats`) and logging.
*   **Infrastructure**: Dockerized for Fly.io, with Neon Postgres database and GitHub Actions CI.

---

## File Structure

### Project Root
*   `ingest.py`: Core logic for inserting normalized events into the database.
*   `backfill_solana.py`: Script to fetch historical swap data from Helius API.
*   `preflight.py`: Startup script to verify environment variables and database connectivity.
*   `config.py`: Central configuration loading (environment variables).
*   `db.py`: Database connection utility for background scripts.
*   `requirements.txt`: Python dependencies.
*   `Dockerfile`: Docker configuration for deployment.
*   `fly.toml`: Fly.io application configuration.

### API (`api/`)
*   `main.py`: FastAPI application entry point. defines routes and health checks.
*   `webhooks.py`: Handles Helius webhook payloads, validates signatures, and triggers ingestion.
*   `metrics.py`: API endpoints for frontend dashboards (e.g., volume, unique makers).
*   `db.py`: Database connection pool for the API.
*   `__init__.py`: Logging configuration.

### Database Schema (`schema/`)
*   `001_initial_schema.sql`: Sets up `events` and `ingestion_stats` tables.
*   `002_add_ignored_reasons.sql`: Adds columns for granular error tracking (missing fields, constraint violations, etc).
*   `003_fix_unique_constraint.sql`: Updates unique constraints to handle multi-leg swaps correctly.

### Tools (`tools/`)
*   `deploy.py`: Deployment orchestrator. Runs DB migrations, deploys to Fly.io, and verifies Helius webhook configuration.
*   `replay_webhook.py`: Developer tool to send sample webhook payloads locally.
*   `sample_webhook_payload.json`: Test data for the replay tool.

### Testing (`tests/`)
*   `test_idempotency.py`: Ensures duplicate events are handled correctly.
*   `test_load.py`: Load testing scripts.

---

## Getting Started

### Prerequisites
*   Python 3.12+
*   PostgreSQL (or Neon account)
*   Helius API Key & Webhook Secret

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/oyololatoni/solana-analytics.git
    cd solana-analytics
    ```

2.  **Set up Virtual Environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure Environment:**
    Copy `.env.example` to `.env` (or `.env.local`) and fill in your credentials.
    ```bash
    cp .env.example .env.local
    ```
    *   `DATABASE_URL`: Postgres connection string.
    *   `HELIUS_WEBHOOK_SECRET`: Secret from your Helius webhook dashboard.
    *   `TRACKED_TOKENS`: Comma-separated list of token mint addresses to track.
    *   `INGESTION_ENABLED`: Set to `1` to enable database writes.

4.  **Initialize Database:**
    Apply the schema migrations manually or using the deploy tool (dry-run).
    ```bash
    # Apply migrations locally
    cat schema/*.sql | psql $DATABASE_URL
    ```

---

## Development

### Running Locally
Start the FastAPI server with hot-reload enabled:

```bash
uvicorn api.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.
*   Health Check: `GET /health`
*   Ingestion Stats: `GET /metrics/ingestion-stats`

### Testing Webhooks
Use the replay tool to simulate a Helius webhook event:

```bash
python tools/replay_webhook.py
```

### Backfilling Data
To fetch historical data for your tracked tokens:

```bash
python backfill_solana.py
```

---

## Deployment

The project includes a unified deployment script `tools/deploy.py` that handles:
1.  **Database Migrations**: Applies pending SQL files to Neon.
2.  **Application Deploy**: Pushes the code to Fly.io.
3.  **Verification**: Checks the live health endpoint and webhook configuration.

### Deploy Command
```bash
python tools/deploy.py
```

**Options:**
*   `--dry-run`: Preview changes without executing.
*   `--db-only`: Only run database migrations.
*   `--skip-db`: Deploy code without checking migrations.

---

## CI/CD

Commits to `main` trigger a GitHub Actions workflow (`.github/workflows/ci.yml`) that:
1.  Sets up a temporary Postgres service.
2.  Runs schema migrations.
3.  Executes `preflight.py` checks.
4.  Runs the test suite (`pytest`).
