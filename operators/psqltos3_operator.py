"""
    Operators to export Postgres tables
    and upload them to S3 bucket
"""

import csv
import io
import os

import awswrangler as wr
from itertools import groupby

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
# from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook


class psqlGetTablesOperator(BaseOperator):
    """
        Get all the table names in PostgresQL database
        and upload it to S3
    """

    def __init__(self, postgres_conn_id: str, s3_conn_id: str, sql_query: str, s3_bucket: str,
                 s3_key: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.sql_query = sql_query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        # table_list = []
        # for i, table_name in enumerate(results):
        #     table_list.append(table_name[0])
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        results = postgres_hook.get_records(self.sql_query)

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
        # s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # load table_names file to get all the table names
        table_file = '/home/airflow/airflow/data/table_names.txt'
        table_list = []
        with open(table_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_list.append(line)

        # enumerate all the tables, put table names into sql query
        for i, table in enumerate(table_list):
            table = table.replace(' ', '')
            sql_query = f"SELECT * FROM {table};"

            # ------ get tables from postgres as
            #        pandas dataframe (for converting purpose) -----
            results = postgres_hook.get_pandas_df(sql_query)

            if results.empty:
                raise ValueError(f"Dataframe for table {table} is empty")

            s3_key_parquet = f"table-parquet/{table}.parquet"
            s3_key_csv = f"table-csv/{table}.csv"
            aws_s3_hook = AwsBaseHook(aws_conn_id=self.s3_conn_id)

            # ----- upload parquet (and csv) to s3 bucket
            wr.s3.to_parquet(df=results,
                             path=f"s3://{self.s3_bucket}/{s3_key_parquet}",
                             boto3_session=aws_s3_hook.get_session())
            wr.s3.to_csv(
                df=results,
                path=f"s3://{self.s3_bucket}/{s3_key_csv}",
                boto3_session=aws_s3_hook.get_session(),
                index=False,
                # dataset=True,  # for table headers
                regular_partitions=True  # for Redshift
            )


def map_postgres_to_redshift_data_type(postgres_data_type):
    """
        Maps PostgreSQL data types to Redshift data types.
    """
    postgres_data_type = postgres_data_type.lower()

    # Define a dictionary that maps PostgreSQL data types to Redshift data types
    POSTGRES_REDSHIFT_MAP = {
        'varchar': 'varchar(256)',
        'character': 'char(256)',
        'text': 'varchar(max)',
        'bigint': 'bigint',
        'integer': 'integer',
        'smallint': 'smallint',
        'boolean': 'boolean',
        'numeric': 'numeric',
        'timestamp': 'timestamp',
        'date': 'date'
    }

    # Extract the data type category (e.g. 'varchar', 'bigint', etc.)
    data_type_category = postgres_data_type.split('(')[0]

    # Map the data type category to the corresponding Redshift data type
    if data_type_category in POSTGRES_REDSHIFT_MAP:
        return POSTGRES_REDSHIFT_MAP[data_type_category]
    else:
        raise ValueError(f"Unknown PostgreSQL data type: {postgres_data_type}")


class getPsqlTableSchemaOperator(PostgresOperator):
    """
    Operator that extracts the schema of a PostgreSQL database
    """

    def __init__(self, postgres_conn_id, schema_filepath, *args, **kwargs):
        self.schema_filepath = schema_filepath
        super().__init__(*args, postgres_conn_id=postgres_conn_id, **kwargs)

    def execute(self, context):
        schema = self.get_schema()
        with open(self.schema_filepath, "w", encoding="UTF-8") as f:
            f.write(schema)
        return self.schema_filepath

    def get_schema(self):
        """
            Returns a SQL script with the schema of the PostgreSQL database
        """
        conn = self.get_db_hook().get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """)
        rows = cursor.fetchall()
        schema = ""
        for table, rows in groupby(rows, lambda x: x[0]):
            schema += f"\nCREATE TABLE {table} (\n"
            schema += ",\n".join(
                [f"    {row[1]} {map_postgres_to_redshift_data_type(row[2])}" for row in rows])
            schema += "\n);"
        return schema
