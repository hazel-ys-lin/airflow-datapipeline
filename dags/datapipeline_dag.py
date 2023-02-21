"""
    DAG to set the workflow of
    processing data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from psqltos3_operator import psqlToS3Operator
from psqltos3_operator import psqlGetTablesOperator


def downloadFromS3(s3_conn_id: str, s3_bucket: str, s3_key: str, local_path: str):
    """_summary_

    Args:
        s3_conn_id (str): _description_
        s3_bucket (str): _description_
        s3_key (str): _description_
        local_path (str): _description_

    Returns:
        _type_: _description_
    """
    s3_hook = S3Hook("aws_s3_conn")
    file_name = s3_hook.download_file(s3_key=s3_key, s3_bucket=s3_bucket, local_path=local_path)
    return file_name


with DAG(
        "psqlToS3",
        default_args={
            "depends_on_past": True,
            "email": ["hazel.ys.lin@viewsonic.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="PostgresQL to S3",
        # scehdule=timedelta(days=1),
        schedule_interval='@daily',
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

    tables_from_s3 = PythonOperator(
        task_id="load_table_names_from_s3",
        python_callable=downloadFromS3,
        op_kwargs={
            "s3_key": "table_names.csv",
            "s3_bucket": "uvs-data-processing-bucket",
            "local_path": "/home/airflow/airflow/data/"
        },
    )

    export_task = psqlToS3Operator(
        task_id="psqltos3",
        postgres_conn_id="uvs_postgres_conn",
        s3_conn_id="aws_s3_conn",
        sql_query="SELECT * FROM user_org;",
        s3_bucket="uvs-data-processing-bucket",
        s3_key="table-csv/user_org.csv",
    )

get_tables_task >> tables_from_s3 >> export_task
