import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'singhrajat',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='setup_dag',
    default_args=dag_default_args,
    schedule_interval= '@once',
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################



query_create_schema = f"""

use bde_at3_1;

create or replace schema raw;
create or replace schema staging;
create or replace schema warehouse;
create or replace schema datamart;

"""


query_setup = f"""
create or replace database bde_at3_1;

use bde_at3_1;


create or replace stage stage_gcp
storage_integration = GCP
url='gcs://australia-southeast1-bde-at-b3605914-bucket/data'
;

create or replace file format file_format_csv 
type = 'CSV' 
field_delimiter = ',' 
skip_header = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

"""




#########################################################
#
#   DAG Operator Setup
#
#########################################################



setup = SnowflakeOperator(
    task_id='query_setup',
    sql=query_setup,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)


create_schema = SnowflakeOperator(
    task_id='query_create_schema',
    sql=query_create_schema,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)



setup >> create_schema

