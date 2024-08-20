from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog import load_catalog
import pandas as pd
import pyarrow as pa

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    hive_catalog = load_catalog(
        "hive", 
        **{
            "uri": "thrift://hive-metastore:9083",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123"
        }
    )
    # Specify your custom logic here
    table = hive_catalog.load_table("lhdew1.dim_user_info")

    scan = table.scan(
        limit=300,
    ).to_pandas()

    print(scan.info())
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
