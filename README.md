# CDC Watermark API

A production-ready, containerized data export system that uses **Change Data Capture (CDC)** principles to efficiently synchronize large datasets. It implements watermarking for stateful processing and exposes REST APIs for **full**, **incremental**, and **delta** export strategies.

## Architecture

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Consumer   │─────▶│  Spring Boot │─────▶│  PostgreSQL  │
│  (API call)  │◀─────│   REST API   │◀─────│   Database   │
└──────────────┘      └──────┬───────┘      └──────────────┘
                             │
                     ┌───────▼───────┐
                     │  CSV Export   │
                     │  (./output/)  │
                     └───────────────┘
```

- **Spring Boot 3.2.4** with Java 17
- **PostgreSQL 13** for persistence
- **Docker & Docker Compose** for containerization
- **JaCoCo** for test coverage reporting

## Quick Start

### Prerequisites
- Docker & Docker Compose installed

### 1. Clone & Configure

```bash
cp .env.example .env
# Edit .env with your desired values (defaults work out of the box)
```

### 2. Run with Docker Compose

```bash
docker-compose up --build
```

This will:
- Start a PostgreSQL 13 database with health checks
- Seed **100,000 users** automatically (idempotent)
- Start the Spring Boot application on port **8080**

### 3. Verify

```bash
curl http://localhost:8080/health
```

## API Endpoints

All export endpoints require the `X-Consumer-ID` header.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/exports/full` | Full export of all non-deleted users |
| `POST` | `/exports/incremental` | Export records changed since last watermark (excludes deleted) |
| `POST` | `/exports/delta` | Export changes with operation type (INSERT/UPDATE/DELETE) |
| `GET` | `/exports/watermark` | Get current watermark for a consumer |

### Example Requests

```bash
# Full export
curl -X POST http://localhost:8080/exports/full -H "X-Consumer-ID: consumer-1"

# Incremental export
curl -X POST http://localhost:8080/exports/incremental -H "X-Consumer-ID: consumer-1"

# Delta export  
curl -X POST http://localhost:8080/exports/delta -H "X-Consumer-ID: consumer-1"

# Check watermark
curl http://localhost:8080/exports/watermark -H "X-Consumer-ID: consumer-1"
```

### Response Examples

**Export Response (202 Accepted):**
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "started",
  "exportType": "full",
  "outputFilename": "full_consumer-1_1709712000000.csv"
}
```

**Watermark Response (200 OK):**
```json
{
  "consumerId": "consumer-1",
  "lastExportedAt": "2026-03-06T07:30:00+05:30"
}
```

## Database Schema

### `users` table
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL (PK) | Unique identifier |
| name | VARCHAR(255) | User's full name |
| email | VARCHAR(255) UNIQUE | User's email |
| created_at | TIMESTAMP WITH TIME ZONE | Record creation time |
| updated_at | TIMESTAMP WITH TIME ZONE | Last update time (indexed) |
| is_deleted | BOOLEAN | Soft delete flag |

### `watermarks` table
| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL (PK) | Unique identifier |
| consumer_id | VARCHAR(255) UNIQUE | Consumer identifier |
| last_exported_at | TIMESTAMP WITH TIME ZONE | Last exported record timestamp |
| updated_at | TIMESTAMP WITH TIME ZONE | Watermark update time |

## Testing

### Run Tests Locally (requires Maven wrapper)

```bash
./mvnw clean test
```

### Run Tests Inside Docker

```bash
docker-compose exec app ./mvnw clean test
```

### Generate Coverage Report

```bash
./mvnw clean test
# Report generated at: target/site/jacoco/index.html
```

Current coverage: **~92%** instruction coverage (Lombok-generated code excluded via `lombok.config`).

## Project Structure

```
cdc-watermark-api/
├── docker-compose.yml          # Service orchestration
├── Dockerfile                  # Multi-stage build
├── .env.example                # Environment variable template
├── lombok.config               # Lombok JaCoCo integration
├── pom.xml                     # Maven dependencies
├── seeds/
│   └── 01-init.sql             # Schema + 100k user seed (idempotent)
├── output/                     # CSV export files (git-ignored)
├── src/
│   ├── main/java/com/software/cdc/
│   │   ├── CdcApplication.java
│   │   ├── controller/
│   │   │   └── ExportController.java
│   │   ├── models/
│   │   │   ├── User.java
│   │   │   └── Watermark.java
│   │   ├── repository/
│   │   │   ├── UserRepository.java
│   │   │   └── WatermarkRepository.java
│   │   └── service/
│   │       └── ExportService.java
│   └── test/java/com/software/cdc/
│       ├── controller/
│       │   └── ExportControllerTest.java
│       └── service/
│           └── ExportServiceTest.java
└── README.md
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | Application port |
| `POSTGRES_USER` | — | Database username |
| `POSTGRES_PASSWORD` | — | Database password |
| `POSTGRES_DB` | — | Database name |
| `DATABASE_URL` | — | Full JDBC-style connection URL |

## Design Decisions

- **Asynchronous exports**: Exports run in background threads (`@Async`) so the API returns `202 Accepted` immediately.
- **Watermark atomicity**: Watermarks are only updated after successful CSV file write within a `@Transactional` boundary.
- **Idempotent seeding**: The seed script checks if data already exists before inserting.
- **Lombok + JaCoCo**: `lombok.config` adds `@Generated` annotations so JaCoCo excludes boilerplate getters/setters from coverage.
- **CDC via timestamps**: Uses `updated_at` column with a database index for efficient incremental queries.
