from pyiceberg.catalog import load_catalog
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.types import (
    TimestampType, FloatType, DoubleType, StringType, NestedField,
    StructType, IntegerType, BooleanType, LongType
)
import pyarrow.compute as pc
from pyiceberg.expressions import EqualTo, And
from datetime import datetime

@custom
def transform_custom(wh_table: dict, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    schema = Schema(
        NestedField(field_id=1, name="base_use_info", field_type=StringType(), required=False),
        NestedField(field_id=2, name="employment_info", field_type=StringType(), required=False),
        NestedField(field_id=3, name="address_info", field_type=StringType(), required=False),
        NestedField(field_id=4, name="subscription_info", field_type=StringType(), required=False),
        NestedField(field_id=5, name="row_id", field_type=StringType(), required=False),
        NestedField(field_id=6, name="datetime_day", field_type=TimestampType(), required=False)
    )
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=6, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )

    sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))

    hive_catalog = load_catalog(
        "hive",
        **{
            "uri": "thrift://hive-metastore:9083",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123"
        }
    )

    arrow_df = pa.Table.from_pandas(wh_table['fact_table'])
    print(arrow_df)
    table = None

    if not hive_catalog.table_exists("lakehouse_w1.fact_user_details1"):
        table = hive_catalog.create_table(
            "lakehouse_w1.fact_user_details1",
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties={
                "downcast-ns-timestamp-to-us-on-write": "true"
            }
        )
    else:
        table = hive_catalog.load_table("lakehouse_w1.fact_user_details1")

    for _, record in arrow_df.to_pandas().iterrows():
        base_use_info = record['base_use_info']
        print(base_use_info)

        existing_df = table.scan(
            row_filter=EqualTo('base_use_info', base_use_info),
        ).to_pandas()

        if not len(existing_df) == 0:
            x = table.delete(delete_filter= EqualTo('base_use_info', base_use_info))
        arrow_df = arrow_df.set_column(
                5,
                'datetime_day',
                arrow_df['datetime_day'].cast(pa.timestamp('us'))
            )
        table.append(arrow_df)

    return {}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
