from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog import load_catalog
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
    IntegerType,
    BooleanType  # Add this for the is_current flag
)

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom( *args, **kwargs):
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

    # Load the table
    table = hive_catalog.load_table("lakehouse_dw1.dim_user_info")

    # Perform a scan to read the data
    arrow_table = table.scan().to_arrow()

    # Convert to Pandas DataFrame for easier manipulation (if needed)
    df = arrow_table.to_pandas()

    # Print the DataFrame
    print('------>',df)

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
