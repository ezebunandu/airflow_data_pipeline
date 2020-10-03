from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    """ Operator to copy data from json files in an s3 bucket
        and move to staging tables in redshift
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 aws_credentials='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn)

        self.log.info('Clearing data from the destination Redshift table')
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_bucket.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        self.log.info(f"copying data from {self.s3_bucket} to Redshift")
        sql = SqlQueries.staging_table_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(sql)
