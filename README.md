# Bikeshare ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline for bikeshare data using Apache Airflow, Docker, and Google Cloud Platform (GCP). The pipeline retrieves bikeshare trips data, transforms it, and loads it into BigQuery for analysis.

## Project Structure

- **airflow.env**: Environment variables for the Airflow instance.
- **docker-compose.yml**: Defines the Docker services for Airflow, including scheduler, web server, and worker.
- **Dockerfile**: Sets up the custom Airflow environment with necessary dependencies.
- **requirements.txt**: Lists Python dependencies required by the project.
- **dags/**: Contains Airflow DAGs.
  - `bikeshare_etl.py`: Primary DAG that orchestrates the ETL process.
- **scripts/**: Supporting scripts and configuration files.
  - `bikeshare_etl.py`: Script for data extraction and transformation.
  - `bikeshare_etl_config.yaml` : Configuration files for ETL parameters.
  - `service_account.json`: Google Cloud service account key for authenticating with GCP.

## Prerequisites

- **Docker** and **Docker Compose** installed.
- **Google Cloud Project** service account with permissions to create and manage BigQuery tables, Dataproc clusters and GCS buckets.

## Setup

1. **Environment Variables**: Create an `airflow.env` file or modify the existing `airflow.env` with necessary environment variables, including `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` with the appropriate values in the format *'{"conn_type": "google_cloud_platform", "extra": {"key_path": "path to service_account.json", "scope": "required scope", "project": "project_id in service_account.json","client_email": "client_email in service_account.json", "num_retries": 5}}'*.

2. **GCP Authentication**: Ensure the service_account.json file is mounted in the Airflow container by placing it inside the scripts folder and set the env_file attribute in the docker-compose.yml file.

3. **ETL Configuration**: Set the following configurations in the bikeshare_etl_config.yaml
    - `initial_load_eq` - Set initial load to "True" when running table for the first time.
                          Setting this attribute will overwrite existing data in the location
    - `manual_load`     - Set manual load to required date if data needs to be loaded for a specific date
    - If both `initial_load_eq` and `manual_load` are not set, latest state will be read from the state path

4. **Build and Start the Containers**:
   ```bash
   docker-compose up -d --build
   
## Dependencies

Install any additional dependencies by adding them to `requirements.txt`.

## Troubleshooting

Database Lock Issues: Restart containers if encountering lock errors.