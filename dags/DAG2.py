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
        'd_customer_retention',
        default_args=args,
        description='dag from part2',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    create_table_f_cust_retention = PostgresOperator(
        task_id='create_table_f_cust_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/DAG2/create_table_f_cust_retention.sql",
    )

    update_f_customer_retention = PostgresOperator(
        task_id='update_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/DAG2/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}})

    (
            create_table_f_cust_retention 
            >> update_f_customer_retention
    )