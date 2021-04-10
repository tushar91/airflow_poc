# -*- coding: utf-8 -*-
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.celery_executor import CeleryExecutor
from datetime import datetime, timedelta
from dags.library_files.library_csv import download_csv_from_url
from dags.subdags.subdag_currency_exchange_to_bigquery import \
    subdag_currency_exchange_to_bigquery


args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 4, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

DAG_NAME = 'currency_exchange_to_bigquery'

dag = airflow.DAG(
    dag_id=DAG_NAME,
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1
)

# DAG to perform all the tasks for the Currency Exchange process

# 1. Download CSV from URL & store in local file
get_csv_from_url = PythonOperator(
    task_id="get_csv_from_url",
    python_callable=download_csv_from_url,
    op_kwargs={
        "csv_url": 'http://webstat.banque-france.fr/fr/'
                   'downloadFile.do?id=5385698&exportType=csv',
        "local_filepath": 'downloads/original_raw_file.csv'
    },
    default_args=args,
    dag=dag
)

# 2. Parse the raw CSV downloaded in the above step to get the dimension
# currency data. Write the data to a CSV which is uploaded to Google
# Cloud Storage & then to BigQuery table
dimension_currency_to_bigquery = SubDagOperator(
    executor=CeleryExecutor(),
    subdag=subdag_currency_exchange_to_bigquery(
        parent_dag_name=DAG_NAME,
        child_dag_name='dimension_currency_to_bigquery',
        execution_date='{{ ds }}',
        flow_name='dimension_currency',
        raw_data_filepath='downloads/original_raw_file.csv',
        destination_project_dataset_table="airflow_poc.raw_dimension_currency",
        schema_fields=[
            {"name": "cur_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "one_euro_value", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "last_updated_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "serial_code", "type": "STRING", "mode": "NULLABLE"}
        ],
        bigquery_table_path="queries/create_dimension_currency.sql",
        final_bigquery_table="airflow_poc.dimension_currency",
        args=args
    ),
    task_id='dimension_currency_to_bigquery',
    default_args=args,
    dag=dag,
)


# 3. Parse the raw CSV downloaded in the above step to get the fact exchange
# rate history data. Write the data to a CSV which is uploaded to Google
# Cloud Storage & then to BigQuery table
fact_exchange_rate_history_to_bigquery = SubDagOperator(
    executor=CeleryExecutor(),
    subdag=subdag_currency_exchange_to_bigquery(
        parent_dag_name=DAG_NAME,
        child_dag_name='fact_exchange_rate_history_to_bigquery',
        execution_date='{{ ds }}',
        flow_name='exchange_rate_history',
        raw_data_filepath='downloads/original_raw_file.csv',
        destination_project_dataset_table="airflow_poc."
                                          "fact_exchange_rate_history",
        schema_fields=[
            {"name": "history_date", "type": "STRING", "mode": "REQUIRED"},
            {"name": "from_cur_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "to_cur_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "exchange_rate", "type": "FLOAT", "mode": "NULLABLE"}
        ],
        bigquery_table_path=None,
        final_bigquery_table=None,
        args=args
    ),
    task_id='fact_exchange_rate_history_to_bigquery',
    default_args=args,
    dag=dag,
)


get_csv_from_url >> dimension_currency_to_bigquery
get_csv_from_url >> fact_exchange_rate_history_to_bigquery
