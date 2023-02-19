from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 statement='',
                 atLeastValue=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.statement=statement
        self.atLeastValue=atLeastValue

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.statement)
        logging.info(f"Logging validation records: {records}")
        if len(records) < self.atLeastValue or len(records[0]) < self.atLeastValue:         
            raise ValueError(f"Data quality check failed. The table returned no result")
        num_records = records[0][0]
        if num_records < self.atLeastValue:
            raise ValueError(f"Data quality check failed. The table contains 0 rows")
        logging.info(f"Data quality check passed with {records[0][0]} table rows")
        
