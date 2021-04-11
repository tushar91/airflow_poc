# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
import json


# Dag will create Airflow connection with GCP
# This DAG will be needed to be executed first on the project


def initialize_gcp_connection(connection_id, gcp_project_name, gcp_key_path):
    """
    Method to initialize Google Cloud Platform Connection
    :param connection_id: ID of the Airflow connection
    :param gcp_project_name: Name of the GCP project
    :param gcp_key_path: Path of the associated GCP key in the project
    Note:
        Modified date: 11-04-2021
        Author: TB
    """

    def create_new_connection(airflow_session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')

        scopes = [
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/cloud-platform",
        ]

        conn_extra = {
            "extra__google_cloud_platform__scope": ",".join(scopes),
            "extra__google_cloud_platform__project": gcp_project_name,
            "extra__google_cloud_platform__key_path": gcp_key_path
        }

        conn_extra_json = json.dumps(conn_extra)
        new_conn.set_extra(conn_extra_json)
        airflow_session.add(new_conn)
        airflow_session.commit()

    session = Session()
    create_new_connection(session,
                            {
                              "conn_id": connection_id,
                              "conn_type": "google_cloud_platform"
                            }
                          )
    session.commit()
    session.close()


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'provide_context': True
}

DAG_NAME = 'airflow_initialize_connections'

dag = airflow.DAG(
    DAG_NAME,
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

create_gcp_connection = PythonOperator(
    task_id='create_gcp_connection',
    python_callable=initialize_gcp_connection,
    op_kwargs={
        "connection_id": "airflow_gcp_connection",
        "gcp_project_name": "airflow-gcp-poc",
        "gcp_key_path": "settings/<file>.json"  # update this with
        # correct file
    },
    provide_context=False,
    dag=dag)
