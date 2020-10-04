from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Operator to perform data quality check
    by confirming that there are records (rows) in
    dimension and fact tables.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=None,
                 redshift_conn='',
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.tables = tables or []
        self.redshift_conn = redshift_conn

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn)
        self.log.info('Starting data quality checks')

        for table in self.tables:
            self.log.info(f'Data quality check on {table}')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = records[0][0]

            if not records or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed.\
                                 {table} returned no results")

            if num_records < 1:
                raise ValueError(f"Data quality check failed.\
                                 {table} contained 0 rows")

            self.log.info(f"Data quality check on table {table} passed with\
                          {num_records} records")
