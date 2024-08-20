from pyiceberg.catalog import load_catalog
import pandas as pd
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.types import (
    TimestampType, FloatType, DoubleType, StringType, NestedField,
    StructType, IntegerType, BooleanType
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

    # Schema Definition
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="uid", field_type=StringType(), required=True),
        NestedField(field_id=3, name="password", field_type=StringType(), required=True),
        NestedField(field_id=4, name="first_name", field_type=StringType(), required=True),
        NestedField(field_id=5, name="last_name", field_type=StringType(), required=True),
        NestedField(field_id=6, name="username", field_type=StringType(), required=True),
        NestedField(field_id=7, name="email", field_type=StringType(), required=True),
        NestedField(field_id=8, name="avatar", field_type=StringType(), required=True),
        NestedField(field_id=9, name="gender", field_type=StringType(), required=True),
        NestedField(field_id=10, name="phone_number", field_type=StringType(), required=True),
        NestedField(field_id=11, name="social_insurance_number", field_type=StringType(), required=True),
        NestedField(field_id=12, name="date_of_birth", field_type=TimestampType(), required=True),
        NestedField(field_id=13, name="credit_card_cc_number", field_type=StringType(), required=True),
        NestedField(field_id=14, name="row_id", field_type=StringType(), required=True),
        NestedField(field_id=15, name="datetime_day", field_type=TimestampType(), required=True),
        # Additional fields for SCD Type 2
        NestedField(field_id=16, name="effective_date", field_type=TimestampType(), required=True),
        NestedField(field_id=17, name="end_date", field_type=TimestampType(), required=False),
        NestedField(field_id=18, name="is_current", field_type=BooleanType(), required=True)
    )

    # Partition Spec Alignment
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=15, field_id=1000, transform=DayTransform(), name="datetime_day"
        )
    )

    # Sort Order Definition
    sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))

    # Hive Catalog Configuration
    hive_catalog = load_catalog(
        "hive",
        **{
            "uri": "thrift://hive-metastore:9083",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123"
        }
    )

    # Convert Pandas DataFrame to PyArrow Table
    arrow_df = pa.Table.from_pandas(wh_table['dim_user_info'])
    table = None
    # Table Creation or Loading
    if not hive_catalog.table_exists("lakehouse_w1.dim_user_info"):
        table = hive_catalog.create_table(
            "lakehouse_w1.dim_user_info",
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties={
                "downcast-ns-timestamp-to-us-on-write": "true"
            }
        )
    else:
        table = hive_catalog.load_table("lakehouse_w1.dim_user_info")
    i = 0
    for _, record in arrow_df.to_pandas().iterrows():
        if i == 1:
            break
        row_id = record['row_id']
        
        # Create filter expressions to match current records
        delete_filter = And(
            EqualTo('row_id', row_id),
            EqualTo('is_current', True)
        )
        
        # Mark old records as inactive
        x = table.delete(delete_filter=delete_filter)
        # print(x)
        
        # Insert new records
        new_record = record.to_dict()
        now = datetime.now()
        new_record['effective_date'] = pd.Timestamp.now().floor('us')
        new_record['end_date'] = None
        new_record['is_current'] = True
        new_record_df = pd.DataFrame([new_record])

        new_record_arrow = pa.Table.from_pandas(new_record_df, preserve_index=False)
        
        new_record_arrow = new_record_arrow.set_column(
            15,
            'effective_date',
            new_record_arrow['effective_date'].cast(pa.timestamp('us'))
        )

        new_record_arrow = new_record_arrow.set_column(
            16,
            'end_date',
            new_record_arrow['end_date'].cast(pa.timestamp('us'))
        )
        print(table.scan().to_pandas())
        print(new_record_arrow)
        table.append(new_record_arrow)
        i+=1
         
    print(hive_catalog.list_tables('lakehouse_w1'))

    return {}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
