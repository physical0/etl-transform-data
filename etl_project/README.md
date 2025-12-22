# ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline with a companion frontend for managing and visualizing the process.

## Implementation Plan

The development roadmap is divided into two main phases:

### Phase 1: Backend ETL Pipeline
Focus on building the core logic for data processing.
1.  **Data Retrieval (Extract)**
    *   Implement extractors for fetching data from:
        *   **S3** (Object Storage)
        *   **External APIs**
2.  **Data Processing (Transform)**
    *   Define **Schemas** for strict data validation (using Pydantic/Pandas).
    *   Apply transformations to clean and structure the data.
3.  **Data Loading (Load)**
    *   Load the processed data into a **PostgreSQL** database.

### Phase 2: Frontend UI
For current basic implementation, we will develop a simple user interface for interaction and monitoring.
1.  **Selection UI**: Allow users to choose the data source (API vs. S3) and target database.
2.  **Execution & Monitoring**: Trigger ETL jobs and view real-time execution logs.
3.  **Data Visualization**: Display the resulting transferred data in a tabular format.

## Directory Layout

The project is planned to be structured as follows:

```text
etl_project/
├── app/
│   ├── etl/              # Core ETL Application Code
│   │   ├── core/         # Core utilities (S3 clients, DB connections)
│   │   ├── models/       # Database models
│   │   ├── pipeline/     # Pipeline orchestration logic
│   │   ├── schemas/      # Pydantic schemas for validation
│   │   ├── services/     # Business logic services
│   │   ├── utils/        # Helper functions
│   ├── static/           # Backend static files
│   ├── api.py            # FastAPI/Flask Backend API entry point
│   ├── Dockerfile        # Backend container definition
│   ├── requirements.txt  # Python dependencies
│   └── run_web.py        # Script to run and test the web server
├── frontend/
│   └── static/           # Frontend static assets
│       ├── css/
│       ├── js/
│       └── index.html    # Main Frontend Entry point
├── docker-compose.yml    # Container orchestration
└── verify_project.py     # Project verification script
```
