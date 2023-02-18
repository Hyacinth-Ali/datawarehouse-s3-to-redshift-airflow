from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info("Clearing existing data in the table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying datasets into the dimension table")
        redshift.run(self.sql)
