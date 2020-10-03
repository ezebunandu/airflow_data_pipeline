from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Operator to load data into the fact tables
    by querying the events and songs staging tables
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 table='',
                 sql='',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn)
        self.log.info(f'Copying into {self.table}')

        redshift.run(self.sql)
