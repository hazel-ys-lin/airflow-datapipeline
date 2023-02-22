"""
    Operators to export Postgres tables
    and upload them to S3 bucket
"""

import csv
import io
import os

import numpy
import pandas
import fastparquet

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook


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
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        # load csv in S3 to get all the table names
        table_file = '/home/airflow/airflow/data/table_names.txt'
        table_list = []
        with open(table_file, 'r', encoding='UTF-8') as file:
            while (line := file.readline().rstrip()):
                table_list.append(line)

        # enumerate all the tables, put table names into sql query
        for i, table in enumerate(table_list):
            table = table.replace(' ', '')
            sql_query = f"SELECT * FROM {table};"
            s3_key = f"table-csv/{table}.parquet"
            results = postgres_hook.get_records(sql_query)

            data_buffer = io.StringIO()
            csv_writer = csv.writer(data_buffer, lineterminator=os.linesep)
            csv_writer.writerows(results)

            # Covert csv to parquet
            parquet_file = csv_writer.to_parquet(f"{table}.parquet")

            data_buffer_binary = io.BytesIO(parquet_file.getvalue().encode())
            s3_hook.load_file_obj(
                file_obj=data_buffer_binary,
                bucket_name=self.s3_bucket,
                key=s3_key,
                replace=True,
            )
