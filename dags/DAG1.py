import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.hooks.http import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'mart_f_sales_new',
        default_args=args,
        description='dag from part1',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    create_table_mart_f_sales_new = PostgresOperator(
        task_id='create_table_mart_f_sales_new',
        postgres_conn_id=postgres_conn_id,
        sql="sql/DAG1/create_table.sql",
    )

    update_mart_f_sales_new = PostgresOperator(
        task_id='update_mart_f_sales_new',
        postgres_conn_id=postgres_conn_id,
        sql="sql/DAG1/mart.f_sales_new.sql",
        parameters={"date": {business_dt}})

    (
            create_table_mart_f_sales_new 
            >> update_mart_f_sales_new
    )