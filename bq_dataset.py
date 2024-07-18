from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

# Set the environment variable for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/kesha/Desktop/gcp_project1/keshanna-123-e1e5758f0829.json'

def create_dataset(dataset_id):
    try:
        
        client = bigquery.Client(project='keshanna-123')
        dataset_ref = client.dataset(dataset_id)

        try:
          
            dataset = client.get_dataset(dataset_ref)
            print(f"The dataset '{dataset_id}' already exists.")

        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = 'US'
            dataset = client.create_dataset(dataset)
            print(f"The dataset '{dataset_id}' is created successfully.")

    except Exception as e:
        print(f"Error creating dataset: {e}")


dataset_id = 'main_dataset'
create_dataset(dataset_id)
