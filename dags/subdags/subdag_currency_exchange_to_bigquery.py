# -*- coding: utf-8 -*-
from airflow.models import DAG
from dags.operators.operator_flow_to_gcs import \
    FlowToGoogleCloudStorage
from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import \
    GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


def subdag_currency_exchange_to_bigquery(parent_dag_name, child_dag_name,
                                         execution_date, flow_name,
                                         raw_data_filepath,
                                         destination_project_dataset_table,
                                         schema_fields,
                                         bigquery_table_path,
                                         final_bigquery_table,
                                         args):
    """
    Subdag which does the following:
    - Parse the raw CSV file to get the dimension currency data & write
    results to a CSV
    - Upload the CSV to GCS
    - Copy data from GCS to BQ
    - Delete file from GCS

    :param parent_dag_name: Main DAG name
    :param child_dag_name: Child DAG name
    :param execution_date: (str) - Airflow execution date
    :param flow_name: (str) - Type of flow to execute:
     - dimension_currency
     - exchange_rate_history
    :param raw_data_filepath: (str) - Raw CSV filepath on Local
    :param destination_project_dataset_table: (str) - BQ table name,
     - dataset.table
    :param schema_fields: (list) - BQ table schema
    :param bigquery_table_path: (str) - BQ table query path
    :param final_bigquery_table: (str) - BQ table name
    :param args: Airflow arguments
    :return: None

    Note:
        Modified date: 10-04-2021
        Author: TB
    """
    dag = DAG(
        f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    )

    # create filename
    filename = f"{flow_name}_{execution_date}.csv"

    # 1. extract data from raw csv file & upload to GCS
    clean_data_to_gcs = FlowToGoogleCloudStorage(
        task_id="clean_data_to_gcs",
        flow_name=flow_name,
        raw_data_filepath=raw_data_filepath,
        clean_filepath=f"downloads/{filename}",
        google_cloud_storage_conn_id="airflow_gcp_connection",
        gcs_bucket="airflow_poc",
        gcs_filepath=f"{flow_name}.csv",
        dag=dag
    )

    # 2. copy file from gcs to bigquery
    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="airflow_poc",
        source_objects=[f"{flow_name}.csv"],
        destination_project_dataset_table=destination_project_dataset_table,
        schema_fields=schema_fields,
        write_disposition="WRITE_TRUNCATE",
        google_cloud_storage_conn_id="airflow_gcp_connection",
        bigquery_conn_id="airflow_gcp_connection",
        dag=dag
    )

    # 3. delete file from GCS
    delete_gcs_file = GoogleCloudStorageDeleteOperator(
        task_id="delete_gcs_file",
        bucket_name="airflow_poc",
        objects=[f"{flow_name}.csv"],
        google_cloud_storage_conn_id="airflow_gcp_connection",
        dag=dag
    )

    clean_data_to_gcs >> gcs_to_bq >> delete_gcs_file

    if bigquery_table_path and final_bigquery_table:
        with open(bigquery_table_path, "r") as q:
            data_query = q.read()

        # create table in bigquery using SQL query
        create_bigquery_table = BigQueryOperator(
            task_id=f"create_bigquery_table",
            sql=data_query,
            destination_dataset_table=final_bigquery_table,
            write_disposition="WRITE_TRUNCATE",
            bigquery_conn_id="airflow_gcp_connection",
            use_legacy_sql=False,
            create_disposition='CREATE_IF_NEEDED',
            time_partitioning=None,
            cluster_fields=None,
            location="EU",
            dag=dag
        )

        clean_data_to_gcs >> gcs_to_bq >> delete_gcs_file
        gcs_to_bq >> create_bigquery_table

    return dag
