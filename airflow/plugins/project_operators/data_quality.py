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
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for dq_check in self.dq_checks:
            test_name = dq_check['name']
            test_sql = dq_check['test_sql']
            expected_result = dq_check['expected_result']
            comparison = dq_check['comparison']

            records = redshift_hook.get_records(test_sql)
            logging.info("Logging validation records: {}:".format(test_name))
            logging.info(records)

            # Check that the result contains at least one record
            if len(records) < 1:         
                    raise ValueError(f"Data quality check failed. The table returned no result")
                
            record = records[0][0]

            # The data quality check supports three comparison: less than, greater than, and equal to
            if comparison == '<':
                if not record < expected_result:
                    raise ValueError("Data quality check failed. The returned value is greater or equal to {}".format(expected_result))
                logging.info("Data quality check passed with returned value: {}".format(record))
            elif comparison == '>':
                if not record > expected_result:
                    raise ValueError("Data quality check failed. The returned value is less or equal to {}".format(expected_result))
                logging.info("Data quality check passed with returned value: {}".format(record))
            else:
                if not record == expected_result:
                    raise ValueError("Data quality check failed. The returned value is not equal to {}".format(expected_result))
                logging.info("Data quality check passed with returned value: {}".format(record))



        
