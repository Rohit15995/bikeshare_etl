import os
import json
import yaml
import datetime as dt
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    )
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)





# Name of the config file
config_file="bikeshare_etl_config"

#getting environment variables
gce_extra=json.loads(os.environ.get('AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'))['extra']
PROJECT=gce_extra['project']
SERVICE_ACCOUNT=gce_extra['client_email']

#Getting configurations
with open(f"/opt/airflow/scripts/{config_file}.yaml") as file:
    etl_options = yaml.safe_load(file)


# ETL configuration
BUCKET_NAME = etl_options['bucket']
DATASET_NAME = etl_options['bigquery_dataset_name']
DATALAKE_PATH = etl_options['datalake_path']
INITIAL_LOAD = etl_options['initial_load_eq']
MANUAL_LOAD = etl_options['manual_load']
TABLE = etl_options['datalake_tables']


# Cluster configuration
CLUSTER_CONFIG=etl_options['cluster_config']
CLUSTER_CONFIG['gce_cluster_config']['service_account']=SERVICE_ACCOUNT
CLUSTER_NAME="bikeshare-etl"


# Default DAG arguments
default_args = {
    'owner': 'airflow',
}
 

#Pyspark job configuration
PYSPARK_FILENAME='bikeshare_etl'
PYSPARK_URI = f"gs://{BUCKET_NAME}/scripts/{PYSPARK_FILENAME}.py"
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
                    "args":[f"-initial_load={INITIAL_LOAD}",
                            f"-manual_load={MANUAL_LOAD}",
                            f"-bucket={BUCKET_NAME}",
                            f"-table={TABLE}",
                            f"-datalake_path={DATALAKE_PATH}"]
                            },
}



with DAG(
    "bikeshare_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=dt.datetime(2024,10,28),
    catchup=False,
    tags=["test"],
) as dag:
 
    # Task to list objects in a GCS bucket
    list_gcs_files = GCSListObjectsOperator(
        task_id="list_gcs_files",
        bucket=BUCKET_NAME,
        
    )
    # Task to upload pyspark file to GCS
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=f"/opt/airflow/scripts/{PYSPARK_FILENAME}.py",
        dst=f"scripts/{PYSPARK_FILENAME}.py",
        bucket=BUCKET_NAME,
        
    )
    
    #Task to create cluster
    create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    cluster_config=CLUSTER_CONFIG,
    region="us-east1",
    cluster_name=CLUSTER_NAME,
    )

    # Task to submit pyspark job
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region="us-east1", 
    )

    # Task to stop cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="stop_cluster",
        gcp_conn_id="google_cloud_default",
        region="us-east1",
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_success"
    )

    # Task to create external table
    for table,schema in etl_options['bigquery_tables'].items():
        create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_{}".format(table),
        destination_project_dataset_table=f"{PROJECT}.{DATASET_NAME}.{table}",
        bucket=BUCKET_NAME,
        source_objects=[DATALAKE_PATH+TABLE+"/*.parquet"],
        schema_fields=schema,
        source_format='PARQUET'
        
        )
    
    # Task to create dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
        )

list_gcs_files >> upload_file >> create_cluster >> pyspark_task >> delete_cluster >> create_dataset >> create_external_table