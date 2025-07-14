from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import pandas as pd
import json

class MinioClient:
    @staticmethod
    def _get():
        # fetch MinIO connection settings from Airflow
        conn = BaseHook.get_connection('minio')
        # initialize MinIO client using endpoint and credentials
        client = Minio(
            endpoint=conn.extra_dejson['endpoint_url'],
            access_key=conn.login,
            secret_key=conn.password,
            secure=False
        )
        return client
    
class CustomMinio:
    @staticmethod
    def _put_csv(dataframe, bucket_name, object_name):
        # convert DataFrame to CSV and encode to bytes
        csv_bytes = dataframe.to_csv(index=False).encode('utf-8')
        # create buffer for upload
        csv_buffer = BytesIO(csv_bytes)

        # upload CSV object to MinIO
        client = MinioClient._get()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=csv_buffer,
            length=len(csv_bytes),
            content_type='application/csv'
        )

    @staticmethod
    def _put_json(json_data, bucket_name, object_name):
        # serialize JSON to string and encode to bytes
        json_string = json.dumps(json_data)
        json_bytes = json_string.encode('utf-8')
        # create buffer for upload
        json_buffer = BytesIO(json_bytes)

        # upload JSON object to MinIO
        client = MinioClient._get()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=json_buffer,
            length=len(json_bytes),
            content_type='application/json'
        )

    @staticmethod
    def _get_dataframe(bucket_name, object_name):
        # download object from MinIO and read into DataFrame
        client = MinioClient._get()
        data = client.get_object(
            bucket_name=bucket_name,
            object_name=object_name
        )
        df = pd.read_csv(data)
        return df
