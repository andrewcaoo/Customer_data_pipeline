

import pandas as pd
from minio import Minio
from io import BytesIO
from pandas import DataFrame
import time
from datetime import datetime, timedelta
import pytz

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        DataFrame: A DataFrame containing data from the files listed in filtered_meta_datas
    """
    bucket_name = 'user-bucket'
    parquet_buffer = BytesIO()

    client = Minio(
        endpoint='minio:9000',
        access_key='minio',
        secret_key='minio123',
        secure=False
    )

    try:
        objects = client.list_objects(bucket_name, recursive=True)

        tz = pytz.utc
        time_24_hours_ago = datetime.now(tz) - timedelta(hours=24)

        filtered_meta_datas = [
            {'file_name': obj.object_name, 'last_modified': obj.last_modified}
            for obj in objects
            if obj.last_modified > time_24_hours_ago
        ]

        
        combined_df = pd.DataFrame()

        for meta_data in filtered_meta_datas:
            obj_name = meta_data['file_name']

            response = client.get_object(bucket_name, obj_name)
            data = response.read()

            df = pd.read_parquet(BytesIO(data))

            combined_df = pd.concat([combined_df, df], ignore_index=True)

        return combined_df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'The output is not a DataFrame'
    assert not output.empty, 'The output DataFrame is empty'