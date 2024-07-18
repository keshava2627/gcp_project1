from google.cloud import bigquery
import os


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/kesha/Desktop/gcp_project1/keshanna-123-e1e5758f0829.json'


client = bigquery.Client()

table_id = 'keshanna-123.main_dataset.main_table'
uri = 'gs://jul-33/products.csv'

job_config = bigquery.LoadJobConfig(
    autodetect=True,  
    skip_leading_rows=1,  
    source_format=bigquery.SourceFormat.CSV,  
)

load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()  


dest_table = client.get_table(table_id)
print(f'Total number of rows in {table_id}: {dest_table.num_rows}')
