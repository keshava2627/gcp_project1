import airflow
from airflow import DAG
from airflow.operators.bash import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.gcs import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.transfers.gcs_to_gcs import *
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import *
from datetime import datetime,timedelta

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure':True,
    'email_on_retry':False,
    'retries': 1,
    'retry_delay':timedelta(minutes=5),

}

dag=DAG(
    'gcs_to_bq',
    default_args=default_args,
    description='gcs_to_bq',
    schedule_interval=None,
    start_date=datetime(2024,7,17),
    catchup=False,

)

dummy_start=DummyOperator(
    task_id='start',
    dag=dag
)


create_bucket=GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name='gcs_to_bq_bucket',
    project_id='keshanna-123',
    dag=dag
    
)


copy_file=GCSToGCSOperator(
    task_id='gcs_gcs_file_cp',
    source_bucket='sales_use_case',
    source_object='customers_1.csv',
    destination_bucket='gcs_to_bq_bucket',
    destination_object='customers_1.csv',
    dag=dag
)

create_dataset=BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset',
    dataset_id='gcs_to_bq_dataset',
    project_id='keshanna-123',
    dag=dag
)

create_table=BigQueryCreateEmptyTableOperator(
    task_id='create_table',
    dataset_id='gcs_to_bq_dataset',
    table_id='gcs_table',
    project_id='keshanna-123',
    dag=dag
)

gcs_to_bq_table=GCSToBigQueryOperator(
    task_id='gcs_to_bq_table',
    bucket='gcs_to_bq_bucket',
    source_objects='customers_1.csv',
    destination_project_dataset_table='gcs_to_bq_dataset.gcs_table',
    write_disposition='WRITE_TRUNCATE',
    source_format='CSV',
    skip_leading_rows=1,
    dag=dag
)
dummy_end=DummyOperator(
    task_id='end',
    dag=dag
)


dummy_start>>create_bucket>>copy_file>>create_dataset>>create_table>>gcs_to_bq_table>>dummy_end