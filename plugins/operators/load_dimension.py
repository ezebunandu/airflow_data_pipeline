from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 table='',
                 append_mode=False,
                 sql='',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn=redshift_conn
        self.table=table
        self.append_mode=append_mode
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn)

        if not self.append_mode:
            self.log.info(f'Not in append mode; clearing the {self.table} before inserting new data')
            redshift.run(f'DELETE FROM {self.table};')

        self.log.info(f'Adding new data to {self.table}')
        redshift.run(self.sql)
