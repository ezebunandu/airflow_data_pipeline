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
                 redshift_conn='',
                 quality_checks=None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.quality_checks = quality_checks or []

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn)
        self.log.info('Starting data quality checks')

        failing_tests = []
        error_count = 0
        for check in self.quality_checks:
            sql = check.get('check_sql')
            expected = check.get('expected_result')

            actual = redshift.get_records(sql)[0][0]

            if expected != actual:
                error_count += 1
                failing_tests.append(sql)

            if error_count > 0:
                self.log.info('Tests failed')
                self.log.info(failing_tests)
                raise ValueError("Data quality check failed")
