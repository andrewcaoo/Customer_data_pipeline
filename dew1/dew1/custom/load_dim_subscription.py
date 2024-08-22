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
        NestedField(field_id=1, name="subscription_plan", field_type=StringType(), required=False),
        NestedField(field_id=2, name="subscription_status", field_type=StringType(), required=False),
        NestedField(field_id=3, name="subscription_payment_method", field_type=StringType(), required=False),
        NestedField(field_id=4, name="subscription_term", field_type=StringType(), required=False),
        NestedField(field_id=5, name="row_id", field_type=StringType(), required=False),
        NestedField(field_id=6, name="datetime_day", field_type=TimestampType(), required=False),

        
        NestedField(field_id=7, name="effective_date", field_type=TimestampType(), required=False),
        NestedField(field_id=8, name="end_date", field_type=TimestampType(), required=False),
        NestedField(field_id=9, name="is_current", field_type=BooleanType(), required=False)
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

    arrow_df = pa.Table.from_pandas(wh_table['dim_subscription'])
    print(arrow_df)
    table = None

    if not hive_catalog.table_exists("lakehouse_w1.dim_subscription"):
        table = hive_catalog.create_table(
            "lakehouse_w1.dim_subscription",
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties={
                "downcast-ns-timestamp-to-us-on-write": "true"
            }
        )
    else:
        table = hive_catalog.load_table("lakehouse_w1.dim_subscription")

    for _, record in arrow_df.to_pandas().iterrows():

        row_id = record['row_id']

        existing_df = table.scan(
            row_filter=EqualTo('row_id', row_id),
        ).to_pandas()

        if not len(existing_df) == 0:
            delete_filter = And(
                EqualTo('row_id', row_id),
                EqualTo('is_current', True)
            )
            x = table.delete(delete_filter=delete_filter)
            existing_df['is_current'] = False
            existing_df['end_date'] = pd.Timestamp.now().floor('us')
            arrow_df = pa.Table.from_pandas(existing_df, preserve_index=False)

            arrow_df = arrow_df.set_column(
                6,
                'effective_date',
                arrow_df['effective_date'].cast(pa.timestamp('us'))
            )

            arrow_df = arrow_df.set_column(
                7,
                'end_date',
                arrow_df['end_date'].cast(pa.timestamp('us'))
            )

            arrow_df = arrow_df.set_column(
                5,
                'datetime_day',
                arrow_df['datetime_day'].cast(pa.timestamp('us'))
            )
            table.append(arrow_df)
        
        new_record = record.to_dict()
        now = datetime.now()
        new_record['effective_date'] = pd.Timestamp.now().floor('us')
        new_record['end_date'] = None
        new_record['is_current'] = True
        new_record_df = pd.DataFrame([new_record])

        new_record_arrow = pa.Table.from_pandas(new_record_df, preserve_index=False)
        
        new_record_arrow = new_record_arrow.set_column(
            6,
            'effective_date',
            new_record_arrow['effective_date'].cast(pa.timestamp('us'))
        )

        new_record_arrow = new_record_arrow.set_column(
            7,
            'end_date',
            new_record_arrow['end_date'].cast(pa.timestamp('us'))
        )
        new_record_arrow = new_record_arrow.set_column(
            5,
            'datetime_day',
            new_record_arrow['datetime_day'].cast(pa.timestamp('us'))
        )
            
        table.append(new_record_arrow)

    return {}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
