from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:/Users/kesha/Desktop/gcp_project1/keshanna-123-e1e5758f0829.json'

def create_bucket(bucket_id):
    client=storage.Client(project='keshanna-123')
    bucket_ref=client.bucket(bucket_id)
    try:
        bucket_ref=client.get_bucket(bucket_id)
        print(f'the bucket {bucket_ref} is already exists.')

    except NotFound:
        print(f"the bucket {bucket_ref} doesn't exists.") 
        bucket_ref=client.create_bucket(bucket_id)
        print(f'the bucket {bucket_ref} is created sucessfully.') 

bucket_id='jul-33'
create_bucket(bucket_id)          
