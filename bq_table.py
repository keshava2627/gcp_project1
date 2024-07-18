
from google.cloud import bigquery
from google.cloud.exceptions import *
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:/Users/kesha/Desktop/gcp_project1/keshanna-123-e1e5758f0829.json'

def create_table(dataset_id,table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table=client.create_table(table)
    print(f'the table {table} is created sucessfully at dataset {dataset_id}')

dataset_id='main_dataset'
table_id='main_table'
create_table(dataset_id,table_id)    