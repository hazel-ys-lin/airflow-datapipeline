from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object: will need this to instantiate a DAG
from airflow import DAG

# Operators: need this to operate
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psqltocsv_operator

with DAG(
    "psqltocsv",
    default_args={
        "depends_on_past": True,
        "email": ["hazel.ys.lin@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Postgres to csv DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 2, 20),
    catchup=False,
    tags=["dataPipeline"],
) as dag:
    extract_task = PythonOperator(
        task_id="psqltocsv",
        python_callable=psqltocsv_operator,
    )
