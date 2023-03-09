"""
    DAG to set the workflow of
    processing data
"""
from datetime import datetime, timedelta
import pendulum
import os
from dotenv import load_dotenv

load_dotenv()

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psqltos3_operator import psqlToS3Operator
from psqltos3_operator import psqlGetTablesOperator
from psqltos3_operator import downloadFromS3Operator
from psqltos3_operator import GetParquetTableSchemaOperator
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
            "email": [os.getenv('EMAIL_ADDRESS')],
            # "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="PostgresQL to S3",
        # schedule=timedelta(hours=12),
        # schedule_interval='@daily',
        schedule_interval='00 09 * * *',
        # schedule_interval=None,
        start_date=pendulum.datetime(2023, 2, 21, tz="Asia/Taipei"),
        catchup=False,
        tags=["dataPipeline"],
) as dag:
    get_tables_task = psqlGetTablesOperator(task_id="get_psql_tables",
                                            postgres_conn_id="uvs_postgres_conn",
                                            s3_conn_id="aws_s3_conn",
                                            s3_bucket="uvs-data-processing-bucket",
                                            s3_key="table_names.csv",
                                            on_failure_callback=handle_failure)

    tables_from_s3_task = downloadFromS3Operator(task_id="load_table_names_from_s3",
                                                 s3_conn_id="aws_s3_conn",
                                                 s3_bucket="uvs-data-processing-bucket",
                                                 s3_key="table_names.csv",
                                                 local_path="/home/airflow/airflow/data/",
                                                 on_failure_callback=handle_failure)

    rename_table_from_s3_task = PythonOperator(task_id="rename_file_from_s3",
                                               python_callable=rename_file,
                                               op_kwargs={'new_name': 'table_names.txt'},
                                               on_failure_callback=handle_failure)

    export_to_s3_task = psqlToS3Operator(
        task_id="psqltos3",
        postgres_conn_id="uvs_postgres_conn",
        s3_conn_id="aws_s3_conn",
        s3_bucket="uvs-data-processing-bucket",
        sla=timedelta(seconds=5),  # Set up timeout length
        on_failure_callback=handle_failure  # Specify the failure handler function
    )

    extract_schema_task = GetParquetTableSchemaOperator(
        task_id="extract_schema_from_db",
        s3_conn_id="aws_s3_conn",
        redshift_conn_id="aws_redshift_conn",
        s3_bucket='uvs-data-processing-bucket',
        redshift_schema_filepath="/home/airflow/airflow/data/uvs_redshift_schema.sql",
        on_failure_callback=handle_failure)

    load_data_to_redshift_task = insertRedshiftFromS3Operator(
        task_id='load_data_to_redshift',
        redshift_conn_id='aws_redshift_conn',
        s3_conn_id='aws_s3_conn',
        s3_bucket='uvs-data-processing-bucket',
        on_failure_callback=handle_failure)

get_tables_task >> tables_from_s3_task >> rename_table_from_s3_task >> export_to_s3_task >> extract_schema_task >> load_data_to_redshift_task
