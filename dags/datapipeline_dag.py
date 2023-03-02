"""
    DAG to set the workflow of
    processing data
"""

import os
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from psqltos3_operator import psqlToS3Operator
from psqltos3_operator import psqlGetTablesOperator
from psqltos3_operator import downloadFromS3Operator
# from psqltos3_operator import getPsqlTableSchemaOperator
from psqltos3_operator import GetParquetTableSchemaOperator
# from psqltos3_operator import createRedshiftTableOperator
from psqltos3_operator import insertRedshiftFromS3Operator


def rename_file(ti, new_name: str) -> None:
    download_file_name = ti.xcom_pull(task_ids=['load_table_names_from_s3'])
    download_file_path = '/'.join(download_file_name[0].split('/')[:-1])
    os.rename(src=download_file_name[0], dst=f"{download_file_path}/{new_name}")


def handle_failure(context):
    task_instance = context['task_instance']
    task_instance.xcom_push(key='failed', value=True)
    # Mark the task as skipped instead of failed
    # task_instance.state = State.SKIPPED


with DAG(
        "psqlToS3",
        default_args={
            "depends_on_past": True,
            # "email": [os.getenv('EMAIL_ADDRESS')],
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="PostgresQL to S3",
        # scehdule=timedelta(days=1),
        # schedule_interval='@daily',
        schedule_interval=None,
        start_date=datetime(2023, 2, 21),
        catchup=False,
        tags=["dataPipeline"],
) as dag:
    get_tables_task = psqlGetTablesOperator(
        task_id="get_psql_tables",
        postgres_conn_id="uvs_postgres_conn",
        s3_conn_id="aws_s3_conn",
        sql_query=
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';",
        s3_bucket="uvs-data-processing-bucket",
        s3_key="table_names.csv",
    )

    tables_from_s3_task = downloadFromS3Operator(
        task_id="load_table_names_from_s3",
        s3_conn_id="aws_s3_conn",
        s3_bucket="uvs-data-processing-bucket",
        s3_key="table_names.csv",
        local_path="/home/airflow/airflow/data/",
    )

    rename_table_from_s3_task = PythonOperator(task_id="rename_file_from_s3",
                                               python_callable=rename_file,
                                               op_kwargs={'new_name': 'table_names.txt'})

    export_to_s3_task = psqlToS3Operator(
        task_id="psqltos3",
        postgres_conn_id="uvs_postgres_conn",
        s3_conn_id="aws_s3_conn",
        s3_bucket="uvs-data-processing-bucket",
        sla=timedelta(seconds=5),  # Set up timeout length
        on_failure_callback=handle_failure  # Specify the failure handler function
    )

    # extract_schema_task = GetParquetTableSchemaOperator(
    #     task_id="extract_schema_from_db",
    #     s3_conn_id="aws_s3_conn",
    #     redshift_conn_id="aws_redshift_conn",
    #     s3_bucket='uvs-data-processing-bucket',
    #     redshift_schema_filepath="/home/airflow/airflow/data/uvs_redshift_schema.sql")

    # create_redshift_tables_task = createRedshiftTableOperator(
    #     task_id='create_tables_redshift',
    #     redshift_conn_id='aws_redshift_conn',
    #     redshift_schema_filepath='/home/airflow/airflow/data/uvs_redshift_schema.sql',
    # )

    load_data_to_redshift_task = insertRedshiftFromS3Operator(
        task_id='load_data_to_redshift',
        redshift_conn_id='aws_redshift_conn',
        s3_conn_id='aws_s3_conn',
        s3_bucket='uvs-data-processing-bucket',
    )

get_tables_task >> tables_from_s3_task >> rename_table_from_s3_task >> export_to_s3_task >> load_data_to_redshift_task
