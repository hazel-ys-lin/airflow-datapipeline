"""
    DAG to set the workflow of
    processing data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import psqltos3_operator

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
    export_task = psqlToS3Operator(
        task_id="psqltos3",
        postgres_conn_id="uvs_postgres_conn",
        s3_conn_id="aws_s3_conn",
        sql_query="SELECT * FROM user_org;",
        s3_bucket="uvs-data-processing-bucket",
        # s3_key="",
    )
