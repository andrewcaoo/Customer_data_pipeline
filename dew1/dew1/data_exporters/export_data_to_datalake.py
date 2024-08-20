import pandas as pd
from minio import Minio
from io import BytesIO
from pandas import DataFrame
import time

@data_exporter
def export_data_to_minio(df: DataFrame, **kwargs) -> None:
    bucket_name = 'user-bucket'
    object_name = str(time.time())+'.parquet'
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    client = Minio(
        endpoint='minio:9000',   
        access_key='minio',              
        secret_key='minio123',              
        secure=False                                 
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    parquet_buffer.seek(0)
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=parquet_buffer,      
        length=parquet_buffer.getbuffer().nbytes,  
        content_type='application/octet-stream'
    )

    print(f"Data exported to MinIO bucket '{bucket_name}' as '{object_name}'")

