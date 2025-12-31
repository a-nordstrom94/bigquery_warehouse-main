# Olist E-commerce ELT Pipeline
### *Orchestrated Data Warehouse with Airflow, dbt, and BigQuery*

[![Airflow 2.10.4](https://img.shields.io/badge/Airflow-2.10.4-017CEE?style=flat&logo=Apache%20Airflow)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8+-FF694B?style=flat&logo=dbt)](https://www.getdbt.com/)
[![GCP BigQuery](https://img.shields.io/badge/GCP-BigQuery-4285F4?style=flat&logo=google-cloud)](https://cloud.google.com/bigquery)

## Overview
This project implements a complete ELT pipeline for the Olist Brazilian E-commerce dataset. It automates the journey from raw CSV data to business-ready analytical "Gold" tables/views, providing a scalable foundation for BI reporting in Looker Studio.
Under assets images from the project can be found

## Airflow 3.x vs 2.x
Originally architected for Airflow 3.x, I made the strategic decision to downgrade to Airflow 2.10.4 after identifying critical stability issues in the 3.x provider ecosystem regarding Docker volume mounting on Windows. This pivot ensured a robust, production-grade environment and highlights my focus on choosing "the right tool for the job" over "the newest tool available."

---

## Architecture


1.  **Orchestration:** Apache Airflow (LocalExecutor) manages task dependencies and scheduling.
2.  **Ingestion (Bronze):** Custom Python scripts/operators load raw Olist data into BigQuery landing tables.
3.  **Transformation (Silver/Gold):** dbt (data build tool) handles SQL transformations:
    * **Silver:** Data cleaning, type casting, and deduplication.
    * **Gold:** Dimensional modeling (Star Schema) for performance-optimized reporting.
4.  **Warehouse:** Google BigQuery serves as the highly-scalable storage and compute engine.
5.  **Visualization:** Looker Studio provides real-time business insights.

---

## 🔗 Service Access
| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| **dbt Documentation** | [http://localhost:8081](http://localhost:8081) | N/A |
| **Looker Studio** | [Public Link Placeholder] | N/A |

---

## Quick Start (Docker)

### Prerequisites
* Docker Compose
* A Google Cloud Service Account (JSON key) with `BigQuery Admin` and `Storage Object Viewer` role permissions (IAM Roles)

### Project Structure
├── airflow/            # DAGs and Airflow configuration
├── dbt_project/        # dbt models, tests, and analysis
├── docker/             # Docker Compose and environment setup
├── raw_data/           # Raw Olist CSV files (for initial GCS upload)
└── assets/             # Documentation images and screenshots

### Setup
1.  **Clone the Repo:**
    ```bash
    git clone https://github.com/a-nordstrom94/bigquery_warehouse.git
    cd bigquery_warehouse/docker
    ```
2.  **Configure Credentials:**
    * Place your GCP JSON key in `docker/keys/service_account.json`.
    * Create a `.env` file based on the provided environment variables.
    * Create a `profiles.yml` file for dbt
3.  **Prepare the Data:**
    This repo includes the raw Olist dataset in the `/data` directory. 
    * Log into your Google Cloud Console.
    * Navigate to Cloud Storage and create a bucket (match the name in your `.env`).
    * Create a folder named **`raw-data`** in the bucket.
    * Upload all CSV files from this repo's `/data` folder into that bucket folder.
4.  **Launch Pipeline:**
    ```bash
    docker compose up -d --build
    ```
5.  **Access Airflow:**
    Open `localhost:8080` (Default Login: `airflow`/`airflow`).
6.  **Access DBT:**
    Open `localhost:8081`

---

## Data Modeling (dbt)
The transformation layer is built on a modular structure to ensure data quality and lineage:

* **`stg_` models:** Light cleaning and renaming of raw inputs.
* **`fct_` / `dim_` models:** Final business entities (Orders, Customers, Products) organized in a Star Schema.

## Data Quality
This project implements a "Tests-First" approach to data engineering. Every run is automatically validated against 35+ data tests to ensure:
* **Integrity:** No duplicate orders or null customer IDs.
* **Consistency:** All Fact table records have valid matching Dimension records.
* **Business Logic:** Unit prices and review scores remain within expected ranges.

---

### Data Lineage
The following lineage graph illustrates the flow from raw source tables (Bronze) through staging (Silver) to the final dimensional models (Gold).

![dbt Lineage Graph](./assets/dbt_lineage_graph.png)

---

## Dashboard & Insights
The final output is an interactive Looker Studio dashboard.

* **Key Metrics:** Total Revenue, Average Order Value (AOV), Customer Lifetime Value (CLV), and Delivery Performance.
* **Dashboard Link:** [View Live Dashboard]([https://your-public-link-here](https://lookerstudio.google.com/u/2/reporting/333b49a4-5784-4e96-8056-d910960ab4d1))

[![Looker Studio Dashboard Preview](./assets/dashboard_preview.png)](https://your-public-link-here)
*Click the image above to view the interactive dashboard.*

---

## Tech Stack & Lessons Learned
* **Infrastructure as Code:** Orchestrated a multi-container environment (Airflow, Postgres, dbt) using Docker Compose.
* **Stability > Novelty:** Decided to pin Airflow to 2.10.4 for production stability over the experimental 3.x release.
* **Container Networking:** Resolved complex volume mapping hurdles between Windows host and Linux containers.
* **Secret Management:** Securely handled GCP credentials using environment variables and volume mounting to prevent sensitive data leaks.

---

## Common problems
### 🔑 Permissions & Linux/WSL2 Issues
If you encounter `Permission Denied` errors (typically when Airflow tries to write to the `/logs` folder):

**Best Practice:** Identify your local User ID by running `id -u` in your terminal. Set this value in your `.env` file: `AIRFLOW_UID=nnnn`. This ensures the containerized Airflow process has the same permissions as your host user.
**Alternative (Development Only):** You can manually adjust the permissions of the local directories to be more permissive using `chmod -R 775 ./airflow/logs`. 
**Note on Security:** Avoid using `chmod 777` in any environment beyond local testing, as it allows global write access to your project files.

### GCS/BQ issues
Make sure you have all the right IAM roles

## To be updated
* Improved dashboard
* SELinux/Apparmor permission support
