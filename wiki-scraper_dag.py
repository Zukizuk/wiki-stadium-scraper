from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from scrape import scrape
from db_utils import *


default_args = {
  'owner': 'Zuki',
  'email': ['marzuk.entsie@amalitech.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(seconds=5),
}

with DAG(
    'wiki-scraper', 
    default_args=default_args, 
    schedule_interval=None, 
    start_date=datetime.now() - timedelta(minutes=5),
    catchup=False,
    tags=['wikipedia', 'scraper']
) as dag: # Define the DAG
  
  scrape_wikipedia = PythonOperator(
    task_id='scrape_wikipedia',
    python_callable=scrape,
  )

  create_raw_stadium_table = PythonOperator(
    task_id='create_table',
    python_callable=create_raw_data_table,
    op_args=['labs.raw_stadium_data', ['rank INT', 'stadium TEXT', 'seating_capacity TEXT', 'region TEXT', 'country TEXT', 'city TEXT', 'images TEXT', 'home_teams TEXT', 'flag TEXT']],
  )

  insert_raw_stadium_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_raw_data,
  )

  validate_wiki_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    op_args=['labs.raw_stadium_data'],
  )

  # create_multiple_tables = PythonOperator(
  #   task_id='create_tables',
  #   python_callable=create_tables_for_normalized_data,
  # )

  # insert_into_normalized_tables = PythonOperator(
  #   task_id='insert_normalized_data',
  #   python_callable=insert_normalized_data
  # )

  scrape_wikipedia >> insert_raw_stadium_data
  create_raw_stadium_table >> insert_raw_stadium_data >> validate_wiki_data 
  

