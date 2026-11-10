# E-Commerce Data Engineering Pipeline

An end-to-end data engineering project that processes e-commerce data using **Python, Snowflake, dbt, Apache Airflow, and Docker**. The pipeline transforms raw customer, order, and shipment data into analytics-ready datasets and monitors delayed orders through automated email notifications.

## Project Overview

E-commerce businesses generate data from multiple sources, including customers, orders, and shipments. This project demonstrates how to build an automated data pipeline that ingests, stores, transforms, and monitors this data.

The pipeline uses Snowflake as the cloud data warehouse, dbt for data transformation, and Apache Airflow for workflow orchestration. It also includes delayed-order monitoring and email alerts using Gmail SMTP.

## Architecture

```text
CSV Files
   |
   v
Python Data Ingestion
   |
   v
Snowflake (RAW Schema)
   |
   v
dbt Transformations
   |
   +--> Staging Models
   |
   +--> Order Status Mart
   |
   v
Apache Airflow Orchestration
   |
   +--> Execute dbt Models
   |
   +--> Check Delayed Orders
             |
             v
       Gmail SMTP Alert
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data generation, ingestion, and monitoring |
| Snowflake | Cloud data warehouse |
| SQL | Data querying and transformation |
| dbt Core | Data transformation and model management |
| Apache Airflow 3.4.0 | Workflow orchestration and scheduling |
| Docker & Docker Compose | Containerization and environment management |
| Gmail SMTP | Email notifications |
| Jupyter Notebook | Sample data generation |

## Project Structure

```text
Ecommerce-data-engineering/
│
├── airflow_project/
│   ├── dags/
│   │   ├── order_monitor_dag.py
│   │   └── utils/
│   │       └── check_delayed_orders.py
│   │
│   ├── config/
│   │   └── dbt/
│   │       └── profiles.yml
│   │
│   ├── Dockerfile
│   └── docker-compose.yaml
│
├── dbt_ecommerce/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_shipments.sql
│   │   │
│   │   └── marts/
│   │       └── order_status.sql
│   │
│   └── dbt_project.yml
│
├── dummy_data_creation/
│   ├── customers.csv
│   ├── orders.csv
│   ├── shipments.csv
│   └── dummy_data_creation.ipynb
│
├── .gitignore
└── README.md
```

## Pipeline Workflow

### 1. Data Generation and Ingestion

- Generate sample e-commerce datasets using Python and Jupyter Notebook.
- Prepare customer, order, and shipment CSV files.
- Load the raw datasets into Snowflake for further processing.

### 2. Data Storage – Snowflake

- Use Snowflake as the central cloud data warehouse.
- Organize data using databases and schemas.
- Store ingested datasets in the RAW schema.
- Configure dbt to connect securely using environment variables.

### 3. Data Transformation – dbt

Create staging models for the raw datasets:

- `stg_customers`
- `stg_orders`
- `stg_shipments`

Create the `order_status` mart by joining customer, order, and shipment data.

The mart calculates delivery duration and identifies delayed orders based on the project's monitoring rule:

- Orders with status `Shipped` and delivery duration greater than 48 hours are classified as `Delayed`.

### 4. Workflow Orchestration – Apache Airflow

Apache Airflow orchestrates the pipeline using a DAG named `order_monitoring_dag`.

The workflow includes:

1. `run_dbt_models` – Executes dbt transformations in Snowflake.
2. `check_orders` – Executes a Python script to check for delayed orders.

The DAG is configured to run hourly.

### 5. Delayed Order Monitoring and Email Alerts

A Python monitoring script connects to Snowflake and queries the `order_status` mart to count delayed orders.

- If delayed orders are found, the task raises an exception.
- Airflow marks the monitoring task as failed.
- A failure callback sends an email notification through Gmail SMTP.

This demonstrates basic data pipeline monitoring and failure notifications.

## Environment Setup

### Prerequisites

- Docker Desktop
- WSL2 with Ubuntu (for Windows users)
- Git
- Snowflake account
- Gmail account with 2-Step Verification enabled

### 1. Clone the Repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
cd Ecommerce-data-engineering
```

### 2. Configure Environment Variables

Create an `.env` file inside `airflow_project/` using the required environment variables.

Example placeholders:

```env
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=your_snowflake_role
SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

Configure the Airflow SMTP connection separately through the Airflow Connections interface.

**Important:** Never commit real passwords, App Passwords, API keys, or other credentials to GitHub.

### 3. Configure dbt

Update the dbt profile configuration in:

`airflow_project/config/dbt/profiles.yml`

The profile uses environment variables to retrieve Snowflake credentials securely.

### 4. Start Airflow

Navigate to the Airflow project directory:

```bash
cd airflow_project
```

Build and start the Docker services:

```bash
docker compose up -d --build
```

Check the running services:

```bash
docker compose ps
```

Access the Airflow UI:

```text
http://localhost:8080
```

### 5. Configure SMTP

In Airflow, navigate to:

**Admin → Connections**

Create an SMTP connection with:

- Connection ID: `smtp_default`
- Host: `smtp.gmail.com`
- Port: `587`
- Login: Your Gmail address
- Password: Gmail App Password

Configure the sender address in the connection's Extra settings.

### 6. Run the Pipeline

Enable and trigger `order_monitoring_dag` in the Airflow UI.

Monitor the task execution and inspect the logs to verify dbt transformations and delayed-order checks.

## Key Features

- End-to-end e-commerce data pipeline.
- Cloud data warehousing using Snowflake.
- SQL transformations and analytics modeling using dbt.
- Workflow orchestration with Apache Airflow.
- Docker-based development environment.
- Automated delayed-order monitoring.
- Email notifications through Gmail SMTP.
- Secure credential configuration using environment variables.

## Future Improvements

- Implement incremental data loading.
- Add dbt data quality tests.
- Introduce logging and improved error handling.
- Add retry strategies and more detailed monitoring.
- Integrate CI/CD using GitHub Actions.
- Build dashboards for e-commerce analytics.

## Learning Outcomes

Through this project, I gained practical experience with:

- Building data pipelines using Python and SQL.
- Working with cloud data warehouses.
- Transforming data using dbt.
- Orchestrating workflows using Apache Airflow.
- Running data engineering services with Docker.
- Implementing basic monitoring and email notifications.

## Author

**Sai Vikas**

Computer Science and Engineering Graduate

GitHub: https://github.com/saivikas499