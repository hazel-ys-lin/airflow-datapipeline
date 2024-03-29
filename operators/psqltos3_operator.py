"""
    Operators to export Postgres tables
    and upload them to S3 bucket
"""

import csv
import io
import os

import pyarrow.parquet as pq
import awswrangler as wr
from dotenv import load_dotenv

load_dotenv()

from airflow.models import BaseOperator

from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# from airflow.exceptions import AirflowException


# TODO: Add error handlers to each class
class psqlGetTablesOperator(BaseOperator):
    """
        Get all the table names in PostgresQL database
        and upload it to S3
    """

    def __init__(self, postgres_conn_id: str, s3_conn_id: str, s3_bucket: str, s3_key: str, *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        sql_query = """
            SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';
        """
        results = postgres_hook.get_records(sql_query)

        data_buffer = io.StringIO()
        csv_writer = csv.writer(data_buffer, lineterminator=os.linesep)
        csv_writer.writerows(results)
        data_buffer_binary = io.BytesIO(data_buffer.getvalue().encode())
        s3_hook.load_file_obj(
            file_obj=data_buffer_binary,
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=True,
        )


class downloadFromS3Operator(BaseOperator):
    """
        Get table names from S3 and download
    """

    def __init__(self, s3_conn_id: str, s3_bucket: str, s3_key: str, local_path: str, *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.local_path = local_path

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        file_name = s3_hook.download_file(
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            local_path=self.local_path,
        )
        return file_name


class psqlToS3Operator(BaseOperator):
    """
        Export data from psql and upload it to s3
    """

    @apply_defaults
    def __init__(self, postgres_conn_id: str, s3_conn_id: str, s3_bucket: str, *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        aws_s3_hook = AwsBaseHook(aws_conn_id=self.s3_conn_id)

        # load table_names file to get all the table names
        table_file = '/home/airflow/airflow/data/table_names.txt'
        table_list = []
        with open(table_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_list.append(line)

        # enumerate all the tables, put table names into sql query
        for i, table in enumerate(table_list):
            table = table.replace(' ', '')

            # Get tables from postgres as pandas dataframe (for converting purpose)
            sql_query = f"SELECT * FROM {table};"
            results = postgres_hook.get_pandas_df(sql_query)

            if results.empty:
                raise ValueError(f"Dataframe for table {table} is empty")

            schema_map = {
                "object": "STRING",
                "datetime64[ns]": "TIMESTAMP",
                "timedelta64": "INTERVAL",
                "int64": "BIGINT",
                "float64": "DOUBLE",
                "bool": "BOOLEAN"
            }

            # schema_dict = {}
            # for column_name, data_type in results.dtypes.iteritems():
            #     if data_type == "object":
            #         schema_dict[column_name] = 'string'
            #     elif data_type == "datetime64[ns]":
            #         schema_dict[column_name] = 'timestamp'
            #     elif data_type == "float64":
            #         schema_dict[column_name] = 'double'
            #     elif data_type == "bool":
            #         schema_dict[column_name] = 'boolean'
            #     else:
            #         print('*************** data_type in for loop: ', data_type)

            schema_dict = {
                column_name: schema_map[str(data_type)]
                for column_name, data_type in results.dtypes.items()
            }
            # print('********** ', table, ' ********** schema_dict: ', schema_dict)

            # Upload parquet to s3 bucket with schema include
            s3_key_parquet = f"table-parquet/{table}.parquet"
            wr.s3.to_parquet(df=results,
                             dtype=schema_dict,
                             path=f"s3://{self.s3_bucket}/{s3_key_parquet}",
                             dataset=False,
                             boto3_session=aws_s3_hook.get_session())


def get_redshift_table_schema(parquet_schema):
    """_summary_:
        Convert parquet schema to Redshift schema

    Args:
        parquet_schema (_type_): class pyarrow schema

    Returns:
        _type_: List of schemas
    """
    redshift_data_types = {
        'BOOL': 'BOOLEAN',
        'INT32': 'INTEGER',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'INT64': 'BIGINT',
        'FLOAT': 'REAL',
        'DOUBLE': 'DOUBLE PRECISION',
        'INTERVAL': 'INTERVAL',
        'STRING': 'VARCHAR',
        'TEXT': 'VARCHAR(65535)',
        'DATE': 'DATE',
        'TIMESTAMP_MICROS': 'TIMESTAMP',
        'TIMESTAMP_MILLIS': 'TIMESTAMP'
    }

    redshift_schema = []
    for field in parquet_schema:
        redshift_type = redshift_data_types.get(str(field.type).upper(), 'VARCHAR')
        if redshift_type == 'VARCHAR':
            redshift_type += '(256)'
        redshift_schema.append(f"{field.name} {redshift_type}")

    return ', '.join(redshift_schema)


class GetParquetTableSchemaOperator(BaseOperator):
    """
        Operator that extracts the schema of parquet files in S3
    """

    def __init__(self, s3_conn_id: str, s3_bucket: str, redshift_conn_id: str,
                 redshift_schema_filepath: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema_filepath = redshift_schema_filepath

    def execute(self, context):
        # Download the parquet file to a temporary file
        aws_s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        aws_redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # load table_names file to get all the table names
        table_file = '/home/airflow/airflow/data/table_names.txt'
        table_list = []
        with open(table_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_list.append(line)

        parquet_dir = "/home/airflow/airflow/data/parquet"

        # Download the parquet file from S3 to local storage
        for table in table_list:
            parquet_key = f"table-parquet/{table}.parquet"
            aws_s3_hook.download_file(parquet_key,
                                      bucket_name=self.s3_bucket,
                                      local_path=parquet_dir,
                                      preserve_file_name=True,
                                      use_autogenerated_subdir=False)

            # Load the Parquet file and extract the schema
            parquet_file = pq.read_table(f"{parquet_dir}/{table}.parquet")
            parquet_schema = parquet_file.schema
            # print('parquet_file: ', parquet_file)

            # Create the Redshift table using the extracted schema
            table_columns = get_redshift_table_schema(parquet_schema)
            # print('********** ', table, ' ********** redshift_table_columns: ', table_columns)
            # print('after convert to redshift datatype: ', table_columns)

            create_table_query = f"""
                DROP TABLE IF EXISTS public.{table};
                CREATE TABLE IF NOT EXISTS public.{table} ({table_columns})
            """

            aws_redshift_hook.run(create_table_query)
            os.remove(f"{parquet_dir}/{table}.parquet")


class insertRedshiftFromS3Operator(BaseOperator):
    """
        Operator that reads the S3 parquet files to input into redshift
    """

    def __init__(self, redshift_conn_id: str, s3_conn_id: str, s3_bucket: str, *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket

    def execute(self, context):
        aws_redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        table_file = '/home/airflow/airflow/data/table_names.txt'
        table_list = []
        with open(table_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_list.append(line)

        for table in table_list:
            s3_key = f"s3://{self.s3_bucket}/table-parquet/{table}.parquet"

            if not aws_s3_hook.check_for_key(s3_key):
                continue

            # FIXME: Pass the tables which contain too long data just for now
            if table in ["avatar", "bidata", "space_editor", "lti_platform_record"]:
                continue

            # generate copy command
            copy_query = f"""
                            TRUNCATE {table};\n\
                            COPY {table}\n\
                            FROM '{s3_key}'\n\
                            IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}'\n\
                            FORMAT AS PARQUET\n\
                            FILLRECORD\n\
                            ;
                        """

            try:
                aws_redshift_hook.run(copy_query)
            except Exception as e:
                raise e
