import io
import pandas as pd
import requests
from pyspark.sql import SparkSession

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pyspark import SparkContext

if SparkContext._active_spark_context is not None:
    sc = SparkContext.getOrCreate()
    sc.stop()

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    try:
        url = 'https://random-data-api.com/api/v2/users?size=100&is_xml=true'
        response = requests.get(url)
        
        if not isinstance(response.json(), list):
            return pd.DataFrame()
        
        df = pd.json_normalize(response.json(), sep='_')

        return df
    except Exception as Ex:
        return pd.DataFrame()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'