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

from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.exceptions import AirflowException
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


class getPsqlTableSchemaOperator(BaseOperator):
    """
        Operator that extracts the schema of a PostgreSQL database
    """

    def __init__(self, postgres_conn_id: str, schema_filepath: str, redshift_schema_filepath: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.schema_filepath = schema_filepath
        self.redshift_schema_filepath = redshift_schema_filepath

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # SQL query to get all table schemas in public schema
        query = """
            SELECT table_name,
                column_name,
                CASE
                    WHEN data_type = 'integer' THEN 'INTEGER'
                    WHEN data_type = 'boolean' THEN 'BOOLEAN'
                    WHEN data_type = 'numeric' THEN 'DECIMAL(' || numeric_precision || ',' || numeric_scale || ')'
                    WHEN data_type = 'character varying' THEN 'VARCHAR(' || character_maximum_length || ')'
                    WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
                    WHEN data_type = 'date' THEN 'DATE'
                    WHEN data_type = 'time without time zone' THEN 'TIME'
                    ELSE data_type
                END AS data_type,
                CASE
                    WHEN is_nullable = 'YES' THEN 'NULL'
                    ELSE 'NOT NULL'
                END AS is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public';
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Write results to file in Redshift schema format
        with open(self.redshift_schema_filepath, 'w', encoding='UTF-8') as f:
            for row in results:
                f.write(f"CREATE TABLE {row[0]} ({row[1]} {row[2]} {row[3]},);\n")

        # Write original schema to file
        with open(self.schema_filepath, 'w', encoding='UTF-8') as f:
            for row in results:
                f.write(f"{row[0]} {row[1]} {row[2]} {row[3]}\n")


class createRedshiftTableOperator(BaseOperator):
    """
        Operator that read the redshift schema sql file to create tables in redshift
    """

    def __init__(self, redshift_conn_id: str, redshift_schema_filepath: str, *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema_filepath = redshift_schema_filepath

    def execute(self, context):
        aws_redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        table_names_file = '/home/airflow/airflow/data/table_names.txt'
        table_names_list = []
        with open(table_names_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_names_list.append(line)

        with open(self.redshift_schema_filepath, 'r', encoding='UTF-8') as schema_file:
            schema_queries = schema_file.read().split(';')

            for query in schema_queries:
                if query.strip() != "":
                    create_query = f"{query.strip()}"
                    aws_redshift_hook.run(create_query)
                    self.log.info(f"Executed {create_query}")
