import logging
import os
import sys

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from dotenv import load_dotenv
import pendulum
from scripts.works_extract import works_extract
from scripts.works_transform import works_transform, authors_transform, sources_transform, topics_transform, keywords_transform
from scripts.data_load import data_load


log = logging.getLogger(__name__)

# loading variables from .env file
load_dotenv('/opt/airflow/config/config.env')
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

archive_folder = "/opt/airflow/archive"
raw_json_path = archive_folder+'/api_response_works.json'
with DAG(
    dag_id="ai_works_dag",
    schedule='@daily',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["testing"]
):
    #extract data for search "artificial intelligence" from API
    extraction_task = PythonOperator(
        task_id = "extraction_task",
        python_callable = works_extract)
    #transform raw to processed
    works_transform_task = PythonOperator(
        task_id = "works_transform_task",
        python_callable = works_transform,
        op_kwargs = {'raw_json_path': raw_json_path,
                     'archive_folder': archive_folder} )    
    authors_transform_task = PythonOperator(
        task_id = "authors_transform_task",
        python_callable = authors_transform,
        op_kwargs = {'raw_json_path': raw_json_path,
                     'archive_folder': archive_folder} )
    sources_transform_task = PythonOperator(
        task_id = "sources_transform_task",
        python_callable = sources_transform,
        op_kwargs = {'raw_json_path': raw_json_path,
                     'archive_folder': archive_folder} )
    topics_transform_task = PythonOperator(
        task_id = "topics_transform_task",
        python_callable = topics_transform,
        op_kwargs = {'raw_json_path': raw_json_path,
                     'archive_folder': archive_folder} )
    keywords_transform_task = PythonOperator(
        task_id = "keywords_transform_task",
        python_callable = keywords_transform,
        op_kwargs = {'raw_json_path': raw_json_path,
                     'archive_folder': archive_folder} )
    #data load
    data_load_task = PythonOperator(
        task_id = "data_load_task",
        python_callable = data_load,
        op_kwargs = {'DB_NAME': DB_NAME, 'DB_USER':DB_USER,
                     'DB_PASS':DB_PASS, 'DB_HOST':DB_HOST,
                     'DB_PORT':DB_PORT, 'archive_folder':archive_folder} )
    
    transform_list = [works_transform_task, authors_transform_task, sources_transform_task,
                      topics_transform_task, keywords_transform_task]
    extraction_task >> transform_list >> data_load_task