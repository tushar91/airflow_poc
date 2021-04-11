# -*- coding: utf-8 -*-
# Source code modified:
# https://airflow.apache.org/docs/stable/_modules/airflow/
# contrib/operators/file_to_gcs.html

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import os
import logging
from dags.library_files.library_csv import write_rows_to_csv
from dags.library_files.library_currency_exchange_rate import \
    dimension_currency_to_csv, exchange_rate_history_to_csv
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class FlowToGoogleCloudStorage(BaseOperator):
    """
    This operator does the following
    - Parse the raw CSV file to get the dimension currency or exchange rate
     history data & write results to a CSV
    - Upload the CSV to GCS

    Note:
        Modified date: 10-04-2021
        Author: TB
    """
    template_fields = ('flow_name', 'raw_data_filepath', 'clean_filepath')

    @apply_defaults
    def __init__(self,
                 flow_name,
                 raw_data_filepath,
                 clean_filepath,
                 google_cloud_storage_conn_id,
                 gcs_bucket,
                 gcs_filepath,
                 *args,
                 **kwargs):
        super(FlowToGoogleCloudStorage, self) \
            .__init__(*args, **kwargs)
        self.flow_name = flow_name
        self.raw_data_filepath = raw_data_filepath
        self.clean_filepath = clean_filepath
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filepath = gcs_filepath
        self.kwargs = kwargs

    def execute(self, context):
        """
        1. Prepare data from the Dimension table, clean & store in CSV on local
        2. Upload the CSV to GCS
        """
        #  depending on the flow name, execute the task
        if self.flow_name == 'dimension_currency':
            # prepare dimension data
            dimension_currency_to_csv(
                self.raw_data_filepath,
                self.clean_filepath
            )
        elif self.flow_name == 'exchange_rate_history':
            # prepare exchnage rate history data
            exchange_rate_history_to_csv(
                self.raw_data_filepath,
                self.clean_filepath
            )
        else:
            raise AirflowException("Incorrect Flow name")

        # upload file to GCS
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id
        )
        hook.upload(
            bucket=self.gcs_bucket,
            object=self.gcs_filepath,
            filename=self.clean_filepath
        )
        logging.info("File uploaded to GCS")

        # remove files
        if os.path.exists(self.clean_filepath):
            os.remove(self.clean_filepath)
        logging.info(f"{self.clean_filepath} : File deleted from local")
